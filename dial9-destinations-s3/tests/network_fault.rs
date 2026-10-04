//! Socket fault at the S3 HTTP connector, driven through the real worker.

use aws_smithy_runtime_api::client::http::{
    HttpConnector, HttpConnectorFuture, SharedHttpConnector, http_client_fn,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use dial9_core::pipeline::{ProcessError, SegmentData, SegmentProcessor};
use dial9_destinations_s3::{S3Config, S3PipelineUploader};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use turmoil_net::fixture::ClientServer;
use turmoil_net::shim::tokio::net::{TcpListener, TcpStream};

const SEGMENT: &[u8] = b"segment";

/// Transmit the SDK's PutObject body over a simulated socket, then wait for
/// a one-byte acknowledgement instead of parsing an HTTP response.
#[derive(Debug)]
struct SimulatedS3Connection;

impl HttpConnector for SimulatedS3Connection {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        assert_eq!(request.method(), "PUT");
        let body = request
            .body()
            .bytes()
            .expect("test segment must be an in-memory SDK body")
            .to_vec();
        HttpConnectorFuture::new(async move {
            let mut stream = TcpStream::connect("s3:9000")
                .await
                .map_err(|e| ConnectorError::io(e.into()))?;
            stream
                .write_all(&body)
                .await
                .map_err(|e| ConnectorError::io(e.into()))?;
            let mut ack = [0];
            stream
                .read_exact(&mut ack)
                .await
                .map_err(|e| ConnectorError::io(e.into()))?;
            assert_eq!(ack, [1]);
            Ok(HttpResponse::new(
                StatusCode::try_from(200).unwrap(),
                SdkBody::empty(),
            ))
        })
    }
}

struct Uploaded(Arc<AtomicUsize>);

impl SegmentProcessor for Uploaded {
    fn name(&self) -> &'static str {
        "Uploaded"
    }

    fn process(
        &mut self,
        data: SegmentData,
    ) -> Pin<Box<dyn Future<Output = Result<SegmentData, ProcessError>> + Send + '_>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(data) })
    }
}

#[test]
fn stalled_s3_response_does_not_prevent_worker_retry() {
    ClientServer::new()
        .server("s3", async {
            let listener = TcpListener::bind("0.0.0.0:9000").await.unwrap();
            let (mut stalled, _) = listener.accept().await.unwrap();
            let mut received = [0; SEGMENT.len()];
            stalled.read_exact(&mut received).await.unwrap();
            assert_eq!(received, SEGMENT);

            // Retain the first connection without replying. The SDK must
            // time out before the next worker attempt can reach this listener.
            let (mut retry, _) = listener.accept().await.unwrap();
            retry.read_exact(&mut received).await.unwrap();
            assert_eq!(received, SEGMENT);
            retry.write_all(&[1]).await.unwrap();
        })
        .run("dial9", async {
            let sdk = aws_sdk_s3::Client::from_conf(
                aws_sdk_s3::Config::builder()
                    .behavior_version_latest()
                    .credentials_provider(aws_sdk_s3::config::Credentials::new(
                        "test", "test", None, None, "test",
                    ))
                    .region(aws_sdk_s3::config::Region::new("us-east-1"))
                    .endpoint_url("http://s3:9000")
                    .force_path_style(true)
                    .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
                    .timeout_config(
                        aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                            .operation_attempt_timeout(Duration::from_millis(100))
                            .build(),
                    )
                    .http_client(http_client_fn(|_, _| {
                        SharedHttpConnector::new(SimulatedS3Connection)
                    }))
                    .build(),
            );
            let config = S3Config::builder()
                .bucket("test-bucket")
                .service_name("test")
                .instance_path("test")
                .region("us-east-1")
                .build();
            let uploaded = Arc::new(AtomicUsize::new(0));
            tokio::time::timeout(
                Duration::from_secs(5),
                dial9_core::test_util::run_pipeline_continuous(
                    vec![SEGMENT.to_vec()],
                    vec![
                        Box::new(S3PipelineUploader::new(config, Some(sdk))),
                        Box::new(Uploaded(Arc::clone(&uploaded))),
                    ],
                    Duration::from_millis(10),
                ),
            )
            .await
            .expect("worker did not finish after S3 recovered")
            .expect("worker failed");
            assert_eq!(uploaded.load(Ordering::SeqCst), 1);
        });
}
