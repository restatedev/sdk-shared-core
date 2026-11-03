#![cfg(feature = "tunnel")]

//! Integration tests for the tunnel engine against a fake, role-reversed
//! Restate Cloud peer speaking plaintext HTTP/2 on localhost.

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use bytes::Bytes;
use ed25519_dalek::{Signer, SigningKey};
use http::{HeaderMap, Method, Request, Response, StatusCode};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Empty, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use restate_sdk_shared_core::cloud::{
    ConnectOptions, ReconnectPolicy, Servers, Tuning, TunnelEngineOptions, CLOUD_KEY_PREFIX,
};
use restate_sdk_shared_core::tunnel::{Error, Event, Status, TunnelBody, TunnelEngine, TunnelInfo};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::task::AbortOnDropHandle;

const TEST_TIMEOUT: Duration = Duration::from_secs(3);
const DISCOVERY_ACCEPT: &str = "application/vnd.restate.endpointmanifest.v2+json";
const IDENTITY_SEED: [u8; 32] = [0x2a; 32];
const LARGE_DISCOVERY_PADDING: usize = 4 * 1024 * 1024;
const TUNNEL_NAME: &str = "greeter-test";

type CloudBody = UnsyncBoxBody<Bytes, Infallible>;

// ───────────────────────────── the SDK side ─────────────────────────────

/// A stand-in for an SDK endpoint: answers `/discover` with a manifest-like body
/// (optionally padded well beyond the HTTP/2 window) and `/invoke/...` with 415,
/// echoing the rewritten path so tests can assert the rewrite.
#[derive(Clone)]
struct TestService {
    padding: usize,
}

impl TestService {
    fn plain() -> Self {
        Self { padding: 0 }
    }

    fn large() -> Self {
        Self {
            padding: LARGE_DISCOVERY_PADDING,
        }
    }

    fn into_service(
        self,
    ) -> impl hyper::service::Service<
        Request<Incoming>,
        Response = Response<TunnelBody>,
        Error = Infallible,
        Future = std::future::Ready<Result<Response<TunnelBody>, Infallible>>,
    > + Clone
           + Send
           + Sync
           + 'static {
        service_fn(move |request: Request<Incoming>| {
            let path = request.uri().path().to_owned();
            let query = request.uri().query().map(str::to_owned);
            let (status, body) = if path == "/discover" {
                let mut body = String::from("{\"protocolMode\":\"BIDI_STREAM\"");
                if self.padding > 0 {
                    body.push_str(",\"padding\":\"");
                    body.push_str(&"x".repeat(self.padding));
                    body.push('"');
                }
                body.push('}');
                (StatusCode::OK, body)
            } else if path.starts_with("/invoke/") {
                (StatusCode::UNSUPPORTED_MEDIA_TYPE, String::new())
            } else {
                (StatusCode::NOT_FOUND, String::new())
            };
            let mut response = Response::new(
                Full::new(Bytes::from(body))
                    .map_err(|never| match never {})
                    .boxed_unsync(),
            );
            *response.status_mut() = status;
            response
                .headers_mut()
                .insert("x-test-path", path.parse().unwrap());
            if let Some(query) = query {
                response
                    .headers_mut()
                    .insert("x-test-query", query.parse().unwrap());
            }
            std::future::ready(Ok::<_, Infallible>(response))
        })
    }
}

fn signing_key() -> SigningKey {
    SigningKey::from_bytes(&IDENTITY_SEED)
}

fn public_key_of(key: &SigningKey) -> String {
    format!(
        "publickeyv1_{}",
        bs58::encode(key.verifying_key().to_bytes()).into_string()
    )
}

fn connect_key(api_key: &str) -> String {
    let query = format!(
        "envid=env_test123&apikey={api_key}&region=eu.restate.cloud&signingkey={}",
        public_key_of(&signing_key())
    );
    format!(
        "{CLOUD_KEY_PREFIX}{}",
        bs58::encode(query.as_bytes()).into_string()
    )
}

fn options_for(servers: Vec<String>, tuning: Tuning) -> TunnelEngineOptions {
    TunnelEngineOptions::resolve_with_env(
        ConnectOptions {
            name: Some(TUNNEL_NAME.into()),
            key: Some(connect_key("test-token")),
            servers: Some(Servers::Explicit(servers)),
            worker_id: Some("worker-test".into()),
            tuning,
            token: None,
        },
        |_| Ok(None),
    )
    .expect("test options resolve")
}

fn options(address: SocketAddr) -> TunnelEngineOptions {
    options_for(vec![format!("http://{address}")], Tuning::default())
}

fn engine(address: SocketAddr) -> TunnelEngine {
    TunnelEngine::start(TestService::plain().into_service(), options(address)).unwrap()
}

fn engine_with(address: SocketAddr, service: TestService) -> TunnelEngine {
    TunnelEngine::start(service.into_service(), options(address)).unwrap()
}

// ───────────────────────────── the Cloud side ─────────────────────────────

type CloudSender = hyper::client::conn::http2::SendRequest<CloudBody>;

struct Handshake {
    status: StatusCode,
    credentials: HeaderMap,
    trailers: oneshot::Sender<HeaderMap>,
}

struct CloudResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

/// A minimal role-reversed Cloud peer. It accepts the SDK's outbound socket,
/// runs HTTP/2 as the client, and keeps the `/_/start-tunnel` request body open
/// until the test supplies request trailers.
struct FakeCloud {
    address: SocketAddr,
    handshake: Option<oneshot::Receiver<(Handshake, CloudSender)>>,
    disconnected: Option<oneshot::Receiver<()>>,
    sender: Option<CloudSender>,
    task: Option<JoinHandle<()>>,
}

impl FakeCloud {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        Self::on(listener).await
    }

    async fn on(listener: TcpListener) -> Self {
        let address = listener.local_addr().unwrap();
        let (handshake_tx, handshake) = oneshot::channel();
        let (disconnected_tx, disconnected) = oneshot::channel();

        let task = tokio::spawn(async move {
            let result = async {
                let (socket, _) = listener.accept().await?;
                let (mut sender, connection) =
                    hyper::client::conn::http2::handshake::<_, _, CloudBody>(
                        TokioExecutor::new(),
                        TokioIo::new(socket),
                    )
                    .await?;
                let mut connection = AbortOnDropHandle::new(tokio::spawn(connection));

                let (trailers, body) = handshake_body();
                let request = Request::builder()
                    .method(Method::GET)
                    .uri("http://fake-tunnel.test/_/start-tunnel")
                    .body(body)
                    .unwrap();
                let observation = {
                    let response = sender.send_request(request).await?;
                    Handshake {
                        status: response.status(),
                        credentials: response.headers().clone(),
                        trailers,
                    }
                };
                let _ = handshake_tx.send((observation, sender.clone()));

                // Keep the h2 driver and request sender alive so tests can
                // model Cloud forwarding more streams on this same session.
                let _ = (&mut connection).await;
                drop(sender);
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            }
            .await;
            if let Err(error) = result {
                eprintln!("fake tunnel peer stopped: {error}");
            }
            let _ = disconnected_tx.send(());
        });

        Self {
            address,
            handshake: Some(handshake),
            disconnected: Some(disconnected),
            sender: None,
            task: Some(task),
        }
    }

    async fn handshake(&mut self) -> Handshake {
        let (handshake, sender) = tokio::time::timeout(
            TEST_TIMEOUT,
            self.handshake.take().expect("handshake requested once"),
        )
        .await
        .expect("SDK did not answer /_/start-tunnel")
        .expect("fake peer stopped before the handshake response");
        self.sender = Some(sender);
        handshake
    }

    async fn wait_for_disconnect(&mut self) {
        tokio::time::timeout(
            TEST_TIMEOUT,
            self.disconnected.take().expect("disconnect requested once"),
        )
        .await
        .expect("tunnel socket was not closed")
        .expect("disconnect notifier was dropped");
        if let Some(task) = self.task.take() {
            task.await.unwrap();
        }
    }

    async fn get(&self, path: &str, headers: HeaderMap) -> CloudResponse {
        let mut sender = self.request_sender();
        let response = tokio::time::timeout(
            TEST_TIMEOUT,
            sender.send_request(cloud_request(path, headers)),
        )
        .await
        .expect("forwarded request did not complete")
        .expect("forwarded request failed");
        let status = response.status();
        let headers = response.headers().clone();
        let body = tokio::time::timeout(TEST_TIMEOUT, response.into_body().collect())
            .await
            .expect("forwarded request did not complete")
            .expect("forwarded response body failed")
            .to_bytes();
        CloudResponse {
            status,
            headers,
            body,
        }
    }

    fn request_sender(&self) -> CloudSender {
        self.sender
            .as_ref()
            .expect("request sender is available after handshake")
            .clone()
    }
}

impl Drop for FakeCloud {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn handshake_body() -> (oneshot::Sender<HeaderMap>, CloudBody) {
    let (trailers_tx, trailers_rx) = oneshot::channel();
    let body = Empty::<Bytes>::new()
        .with_trailers(async move { trailers_rx.await.ok().map(Ok::<_, Infallible>) })
        .boxed_unsync();
    (trailers_tx, body)
}

fn cloud_request(path: &str, headers: HeaderMap) -> Request<CloudBody> {
    let mut request = Request::builder()
        .method(Method::GET)
        .uri(format!("http://fake-tunnel.test{path}"))
        .body(Empty::<Bytes>::new().boxed_unsync())
        .unwrap();
    *request.headers_mut() = headers;
    request
}

fn sign_identity(audience: &str) -> String {
    sign_identity_with(audience, &signing_key())
}

fn sign_identity_with(audience: &str, key: &SigningKey) -> String {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch")
        .as_secs();
    let header = format!(
        "{{\"alg\":\"EdDSA\",\"typ\":\"JWT\",\"kid\":\"{}\"}}",
        public_key_of(key)
    );
    let claims = format!(
        "{{\"aud\":\"{audience}\",\"nbf\":{},\"iat\":{now},\"exp\":{}}}",
        now.saturating_sub(60),
        now.saturating_add(60)
    );
    let signed = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(header),
        URL_SAFE_NO_PAD.encode(claims)
    );
    let signature = key.sign(signed.as_bytes());
    format!("{signed}.{}", URL_SAFE_NO_PAD.encode(signature.to_bytes()))
}

fn identity_headers(audience: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("accept", DISCOVERY_ACCEPT.parse().unwrap());
    headers.insert("x-restate-signature-scheme", "v1".parse().unwrap());
    headers.insert("x-restate-jwt-v1", sign_identity(audience).parse().unwrap());
    headers
}

fn ok_trailers() -> HeaderMap {
    ok_trailers_for(TUNNEL_NAME)
}

fn ok_trailers_for(name: &str) -> HeaderMap {
    let mut trailers = HeaderMap::new();
    trailers.insert("tunnel-status", "ok".parse().unwrap());
    trailers.insert("tunnel-name", name.parse().unwrap());
    trailers.insert(
        "proxy-url",
        format!("https://proxy.example/env_test123/{name}")
            .parse()
            .unwrap(),
    );
    trailers.insert(
        "tunnel-url",
        format!("https://tunnel.example/{name}").parse().unwrap(),
    );
    trailers
}

fn assert_credentials(handshake: &Handshake) {
    assert_eq!(handshake.status, StatusCode::OK);
    assert_eq!(handshake.credentials["authorization"], "Bearer test-token");
    assert_eq!(handshake.credentials["environment-id"], "env_test123");
    assert_eq!(handshake.credentials["tunnel-name"], TUNNEL_NAME);
    assert_eq!(handshake.credentials["tunnel-worker-id"], "worker-test");
    assert_eq!(handshake.credentials["supports-drain"], "true");
    assert_eq!(handshake.credentials["supports-client-drain"], "true");

    let connection_id = handshake.credentials["tunnel-connection-id"]
        .to_str()
        .unwrap();
    uuid::Uuid::parse_str(connection_id).expect("connection id must be a UUID");

    let credential_names = handshake
        .credentials
        .keys()
        .map(|name| name.as_str())
        // Hyper may supply generic representation metadata. Everything else
        // in these response headers belongs to the tunnel credentials.
        .filter(|name| !matches!(*name, "date" | "content-length"))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        credential_names,
        BTreeSet::from([
            "authorization",
            "environment-id",
            "supports-client-drain",
            "supports-drain",
            "tunnel-connection-id",
            "tunnel-name",
            "tunnel-worker-id",
        ])
    );
}

async fn established(cloud: &mut FakeCloud) -> (TunnelEngine, TunnelInfo) {
    established_for(cloud, engine(cloud.address)).await
}

async fn established_for(
    cloud: &mut FakeCloud,
    engine: TunnelEngine,
) -> (TunnelEngine, TunnelInfo) {
    let ready = AbortOnDropHandle::new(tokio::spawn({
        let engine = engine.clone();
        async move { engine.ready().await }
    }));
    let handshake = cloud.handshake().await;
    assert_credentials(&handshake);

    // Receiving response headers is not readiness. Cloud authorization is
    // carried in request trailers, so ready must remain pending here.
    tokio::task::yield_now().await;
    assert!(!ready.is_finished(), "ready returned before Cloud trailers");
    assert_eq!(*engine.status().borrow(), Status::Starting);

    handshake.trailers.send(ok_trailers()).unwrap();
    let info = tokio::time::timeout(TEST_TIMEOUT, ready)
        .await
        .expect("ready did not observe successful trailers")
        .unwrap()
        .unwrap();
    (engine, info)
}

// ───────────────────────────── lifecycle ─────────────────────────────

#[tokio::test]
async fn role_reversed_handshake_waits_for_trailers() {
    let mut cloud = FakeCloud::start().await;
    let (engine, info) = established(&mut cloud).await;

    assert_eq!(info.name, TUNNEL_NAME);
    assert_eq!(
        info.proxy_url,
        "https://proxy.example/env_test123/greeter-test"
    );
    assert_eq!(info.tunnel_url, "https://tunnel.example/greeter-test");
    assert_eq!(
        info.deployment_url,
        "https://proxy.example:9080/env_test123/greeter-test/http/in-process/9080/"
    );
    assert_eq!(*engine.status().borrow(), Status::Ready(info.clone()));
    // Ready is idempotent and immediate once established.
    assert_eq!(engine.ready().await.unwrap(), info);

    let close = engine.shutdown(Duration::ZERO);
    cloud.wait_for_disconnect().await;
    close.await.unwrap();
    assert!(matches!(
        &*engine.status().borrow(),
        Status::Terminated {
            result: Ok(()),
            info: Some(_)
        }
    ));
    assert_eq!(engine.terminated().await, Ok(()));
}

#[tokio::test]
async fn status_and_events_follow_the_lifecycle() {
    let mut cloud = FakeCloud::start().await;
    let engine = engine(cloud.address);
    let mut events = engine.events();
    let mut status = engine.status();

    let (engine, info) = established_for(&mut cloud, engine).await;

    let first = tokio::time::timeout(TEST_TIMEOUT, events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        first,
        Event::Connecting {
            target: cloud.address.to_string()
        }
    );
    let second = tokio::time::timeout(TEST_TIMEOUT, events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        second,
        Event::Connected {
            target: cloud.address.to_string(),
            info: info.clone()
        }
    );

    status.changed().await.unwrap();
    assert_eq!(*status.borrow_and_update(), Status::Ready(info));

    engine.shutdown(Duration::ZERO).await.unwrap();
    status.changed().await.unwrap();
    assert!(matches!(
        &*status.borrow_and_update(),
        Status::Terminated { result: Ok(()), .. }
    ));
}

#[tokio::test]
async fn cancelling_while_trailers_are_pending_closes_the_session() {
    let mut cloud = FakeCloud::start().await;
    let engine = engine(cloud.address);
    let handshake = cloud.handshake().await;
    assert_credentials(&handshake);

    // Keep Cloud's request body open: dropping the engine must tear down the
    // handshake reader, the H2 driver, and the socket without waiting for
    // trailers or for the handshake deadline.
    drop(engine);
    cloud.wait_for_disconnect().await;
    drop(handshake);
}

#[tokio::test]
async fn shutdown_before_ready_reports_closed() {
    let mut cloud = FakeCloud::start().await;
    let engine = engine(cloud.address);
    let handshake = cloud.handshake().await;
    assert_credentials(&handshake);

    let ready = AbortOnDropHandle::new(tokio::spawn({
        let engine = engine.clone();
        async move { engine.ready().await }
    }));
    engine.shutdown(Duration::from_secs(1)).await.unwrap();
    assert_eq!(ready.await.unwrap(), Err(Error::Closed));
    cloud.wait_for_disconnect().await;
    drop(handshake);
}

#[tokio::test]
async fn forwarding_rewrites_before_identity_verification() {
    let mut cloud = FakeCloud::start().await;
    let (engine, _) = established(&mut cloud).await;

    let response = cloud
        .get("/http/h/9080/discover?x=1", identity_headers("/discover"))
        .await;
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.headers["x-test-path"], "/discover");
    assert_eq!(response.headers["x-test-query"], "x=1");
    assert!(std::str::from_utf8(&response.body)
        .unwrap()
        .contains("BIDI_STREAM"));

    // A valid signed invocation reaches the service after the rewrite.
    let response = cloud
        .get(
            "/http/h/9080/invoke/Greeter/greet",
            identity_headers("/invoke/Greeter/greet"),
        )
        .await;
    assert_eq!(response.status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(response.headers["x-test-path"], "/invoke/Greeter/greet");

    // Health and drain are answered by the engine itself, unsigned.
    let response = cloud.get("/_/health", HeaderMap::new()).await;
    assert_eq!(response.status, StatusCode::OK);
    assert!(!response.headers.contains_key("x-test-path"));

    let mut missing_identity = HeaderMap::new();
    missing_identity.insert("accept", DISCOVERY_ACCEPT.parse().unwrap());
    let response = cloud.get("/http/h/9080/discover", missing_identity).await;
    assert_eq!(response.status, StatusCode::UNAUTHORIZED);
    assert!(!response.headers.contains_key("x-test-path"));

    // The audience is the rewritten path, never the forwarded one.
    let response = cloud
        .get(
            "/http/h/9080/discover",
            identity_headers("/http/h/9080/discover"),
        )
        .await;
    assert_eq!(response.status, StatusCode::UNAUTHORIZED);

    let mut wrong_key = identity_headers("/discover");
    wrong_key.insert(
        "x-restate-jwt-v1",
        sign_identity_with("/discover", &SigningKey::from_bytes(&[0x7c; 32]))
            .parse()
            .unwrap(),
    );
    let response = cloud.get("/http/h/9080/discover", wrong_key).await;
    assert_eq!(response.status, StatusCode::UNAUTHORIZED);

    let response = cloud
        .get("/http/h/not-a-port/discover", identity_headers("/discover"))
        .await;
    assert_eq!(response.status, StatusCode::BAD_REQUEST);

    let close = engine.shutdown(Duration::ZERO);
    cloud.wait_for_disconnect().await;
    close.await.unwrap();
}

#[tokio::test]
async fn graceful_shutdown_closes_the_role_reversed_session() {
    let mut cloud = FakeCloud::start().await;
    let (engine, _) = established(&mut cloud).await;

    // The lifecycle contract requires drain to begin when the method is
    // called, not when its returned future is first polled.
    let shutdown = engine.shutdown(Duration::from_secs(1));
    cloud.wait_for_disconnect().await;
    shutdown.await.unwrap();
}

#[tokio::test]
async fn graceful_shutdown_refuses_raced_requests_then_flushes_in_flight_response() {
    let mut cloud = FakeCloud::start().await;
    let engine = engine_with(cloud.address, TestService::large());
    let (engine, _) = established_for(&mut cloud, engine).await;

    // Leave a response much larger than the HTTP/2 receive window unread.
    // The server has observed response-body EOS, but the peer has not yet
    // received all DATA, so graceful shutdown must keep the session alive.
    let mut held_sender = cloud.request_sender();
    let held_response = tokio::time::timeout(
        TEST_TIMEOUT,
        held_sender.send_request(cloud_request(
            "/http/h/9080/discover",
            identity_headers("/discover"),
        )),
    )
    .await
    .expect("held request did not receive response headers")
    .expect("held request failed");
    assert_eq!(held_response.status(), StatusCode::OK);

    // `send_request` queues the new stream synchronously. Calling shutdown
    // before polling its response models a stream racing the client's GOAWAY:
    // it must see the draining sentinel if it reaches the SDK.
    let mut raced_sender = cloud.request_sender();
    let raced_response = raced_sender.send_request(cloud_request(
        "/http/h/9080/discover",
        identity_headers("/discover"),
    ));
    let shutdown = engine.shutdown(Duration::from_secs(2));

    let raced_response = tokio::time::timeout(TEST_TIMEOUT, raced_response)
        .await
        .expect("raced request did not complete")
        .expect("raced stream was reset instead of receiving the drain sentinel");
    assert_eq!(raced_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        raced_response.headers()["x-restate-tunnel-draining"],
        "true"
    );
    raced_response
        .into_body()
        .collect()
        .await
        .expect("draining response body failed");

    let mut shutdown = Box::pin(shutdown);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut shutdown)
            .await
            .is_err(),
        "shutdown completed before the unread response was flushed"
    );

    let held_body = tokio::time::timeout(TEST_TIMEOUT, held_response.into_body().collect())
        .await
        .expect("held response was not flushed during the grace period")
        .expect("held response body failed")
        .to_bytes();
    assert!(held_body.len() >= LARGE_DISCOVERY_PADDING);

    tokio::time::timeout(TEST_TIMEOUT, &mut shutdown)
        .await
        .expect("shutdown did not complete after the response flushed")
        .unwrap();
    cloud.wait_for_disconnect().await;
}

/// A service whose response body stays open until released: an invocation in flight.
#[derive(Clone)]
struct HeldService {
    release: Arc<tokio::sync::Notify>,
}

impl hyper::service::Service<Request<Incoming>> for HeldService {
    type Response = Response<TunnelBody>;
    type Error = Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Infallible>> + Send>,
    >;

    fn call(&self, _request: Request<Incoming>) -> Self::Future {
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            let (sender, receiver) = tokio::sync::mpsc::channel::<
                Result<http_body::Frame<Bytes>, restate_sdk_shared_core::tunnel::BoxError>,
            >(1);
            tokio::spawn(async move {
                release.notified().await;
                let _ = sender
                    .send(Ok(http_body::Frame::data(Bytes::from_static(b"done"))))
                    .await;
            });
            let body = http_body_util::StreamBody::new(
                tokio_stream::wrappers::ReceiverStream::new(receiver),
            );
            Ok(Response::new(body.boxed_unsync()))
        })
    }
}

#[tokio::test]
async fn graceful_shutdown_waits_for_a_response_body_still_being_produced() {
    let mut cloud = FakeCloud::start().await;
    let release = Arc::new(tokio::sync::Notify::new());
    let service = HeldService {
        release: Arc::clone(&release),
    };
    let engine = TunnelEngine::start(service, options(cloud.address)).unwrap();
    let (engine, _) = established_for(&mut cloud, engine).await;

    let mut sender = cloud.request_sender();
    let in_flight = tokio::time::timeout(
        TEST_TIMEOUT,
        sender.send_request(cloud_request(
            "/http/h/9080/slow",
            identity_headers("/slow"),
        )),
    )
    .await
    .expect("in-flight request did not receive response headers")
    .expect("in-flight request failed");
    assert_eq!(in_flight.status(), StatusCode::OK);

    let shutdown = engine.shutdown(Duration::from_secs(5));
    tokio::pin!(shutdown);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut shutdown)
            .await
            .is_err(),
        "shutdown completed while a response body was still being produced"
    );

    release.notify_one();
    let body = tokio::time::timeout(TEST_TIMEOUT, in_flight.into_body().collect())
        .await
        .expect("in-flight response was not completed")
        .expect("in-flight response failed")
        .to_bytes();
    assert_eq!(body, "done");
    tokio::time::timeout(TEST_TIMEOUT, &mut shutdown)
        .await
        .expect("shutdown did not complete after the response")
        .unwrap();
    cloud.wait_for_disconnect().await;
}

#[tokio::test]
async fn graceful_shutdown_force_closes_a_peer_that_does_not_read() {
    let mut cloud = FakeCloud::start().await;
    let engine = engine_with(cloud.address, TestService::large());
    let (engine, _) = established_for(&mut cloud, engine).await;

    let mut sender = cloud.request_sender();
    let held_response = tokio::time::timeout(
        TEST_TIMEOUT,
        sender.send_request(cloud_request(
            "/http/h/9080/discover",
            identity_headers("/discover"),
        )),
    )
    .await
    .expect("held request did not receive response headers")
    .expect("held request failed");
    assert_eq!(held_response.status(), StatusCode::OK);

    tokio::time::timeout(TEST_TIMEOUT, engine.shutdown(Duration::from_millis(30)))
        .await
        .expect("grace deadline did not force-close the tunnel")
        .unwrap();
    cloud.wait_for_disconnect().await;

    // Keeping the body alive until after disconnect is the condition this
    // test exercises; dropping it now cannot be what allowed shutdown.
    drop(held_response);
}

#[tokio::test]
async fn dropping_the_last_handle_abruptly_closes_the_session() {
    let mut cloud = FakeCloud::start().await;
    let (engine, _) = established(&mut cloud).await;
    let clone = engine.clone();

    drop(engine);
    // A clone keeps the engine alive.
    assert_eq!(
        cloud.get("/_/health", HeaderMap::new()).await.status,
        StatusCode::OK
    );
    drop(clone);
    cloud.wait_for_disconnect().await;
}

#[tokio::test]
async fn dropping_unpolled_shutdown_future_cannot_prevent_cleanup() {
    let mut cloud = FakeCloud::start().await;
    let (engine, _) = established(&mut cloud).await;

    let shutdown = engine.shutdown(Duration::from_secs(1));
    drop(shutdown);
    cloud.wait_for_disconnect().await;
    assert_eq!(engine.terminated().await, Ok(()));
}

// ───────────────────────────── handshake taxonomy ─────────────────────────────

#[derive(Clone, Copy)]
enum HandshakeResult {
    Unauthorized,
    TooManyTunnels,
    UnexpectedData,
    Ok,
}

impl HandshakeResult {
    fn trailers(self) -> HeaderMap {
        let mut trailers = HeaderMap::new();
        match self {
            Self::Unauthorized => {
                trailers.insert("tunnel-status", "unauthorized".parse().unwrap());
            }
            Self::TooManyTunnels => {
                trailers.insert("tunnel-status", "too-many-tunnels".parse().unwrap());
            }
            Self::UnexpectedData => unreachable!("DATA attempts do not send trailers"),
            Self::Ok => return ok_trailers(),
        }
        trailers
    }
}

struct HandshakeObservation {
    status: StatusCode,
    credentials: HeaderMap,
}

/// A scripted role-reversed H2 peer which owns every connection driver it
/// starts. Each script entry applies to one newly accepted SDK dial.
struct ScriptedCloud {
    address: SocketAddr,
    observations: mpsc::Receiver<HandshakeObservation>,
    task: Option<JoinHandle<()>>,
}

impl ScriptedCloud {
    async fn start(script: Vec<HandshakeResult>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (observation_tx, observations) = mpsc::channel(script.len());

        let task = tokio::spawn(async move {
            for result in script {
                let (socket, _) = tokio::time::timeout(TEST_TIMEOUT, listener.accept())
                    .await
                    .expect("SDK did not dial the scripted Cloud peer")
                    .unwrap();
                let (mut sender, connection) =
                    hyper::client::conn::http2::handshake::<_, _, CloudBody>(
                        TokioExecutor::new(),
                        TokioIo::new(socket),
                    )
                    .await
                    .unwrap();
                // If this outer script task is aborted, the driver is aborted
                // too instead of becoming a detached Tokio task.
                let connection = AbortOnDropHandle::new(tokio::spawn(connection));

                let (trailers_tx, body) = if matches!(result, HandshakeResult::UnexpectedData) {
                    // More than one maximum-sized H2 frame proves the SDK
                    // rejects the first DATA frame instead of collecting the
                    // handshake request body without a bound.
                    (
                        None,
                        Full::new(Bytes::from(vec![b'x'; 128 * 1024])).boxed_unsync(),
                    )
                } else {
                    let (trailers_tx, body) = handshake_body();
                    (Some(trailers_tx), body)
                };
                let request = Request::builder()
                    .method(Method::GET)
                    .uri("http://fake-tunnel.test/_/start-tunnel")
                    .body(body)
                    .unwrap();
                let response = sender.send_request(request).await.unwrap();
                observation_tx
                    .send(HandshakeObservation {
                        status: response.status(),
                        credentials: response.headers().clone(),
                    })
                    .await
                    .unwrap();
                if let Some(trailers_tx) = trailers_tx {
                    trailers_tx.send(result.trailers()).unwrap();
                }

                // A rejected attempt must close before the slot redials; the
                // successful final attempt remains here until the test shuts
                // the engine down. Keeping `sender` alive ensures the close is
                // initiated by the SDK rather than this fake peer.
                let _driver_result = tokio::time::timeout(TEST_TIMEOUT, connection)
                    .await
                    .expect("SDK did not close a completed handshake attempt")
                    .expect("fake Cloud H2 driver task panicked");
                drop(sender);
            }
        });

        Self {
            address,
            observations,
            task: Some(task),
        }
    }

    async fn observation(&mut self) -> HandshakeObservation {
        tokio::time::timeout(TEST_TIMEOUT, self.observations.recv())
            .await
            .expect("SDK did not answer the scripted start request")
            .expect("script ended before the expected start response")
    }

    async fn finish(&mut self) {
        let task = AbortOnDropHandle::new(self.task.take().expect("script task awaited once"));
        tokio::time::timeout(TEST_TIMEOUT, task)
            .await
            .expect("scripted Cloud peer did not finish")
            .expect("scripted Cloud peer task panicked");
    }
}

impl Drop for ScriptedCloud {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn assert_start_response(observation: &HandshakeObservation) {
    assert_eq!(observation.status, StatusCode::OK);
    assert_eq!(
        observation.credentials["authorization"],
        "Bearer test-token"
    );
    assert_eq!(observation.credentials["environment-id"], "env_test123");
    assert_eq!(observation.credentials["tunnel-name"], TUNNEL_NAME);
}

#[tokio::test]
async fn unauthorized_is_fatal_before_readiness() {
    let mut cloud = ScriptedCloud::start(vec![HandshakeResult::Unauthorized]).await;
    let engine = engine(cloud.address);
    let mut events = engine.events();

    let observation = cloud.observation().await;
    assert_start_response(&observation);

    let error = tokio::time::timeout(TEST_TIMEOUT, engine.ready())
        .await
        .expect("fatal handshake did not terminate ready")
        .expect_err("unauthorized handshake unexpectedly established");
    assert!(
        error.to_string().contains("unauthorized"),
        "fatal error should retain the non-secret status: {error}"
    );
    assert_eq!(engine.terminated().await, Err(error.clone()));
    assert!(matches!(
        &*engine.status().borrow(),
        Status::Terminated {
            result: Err(_),
            info: None
        }
    ));
    // Shutting down an engine that already stopped reports its terminal result.
    assert_eq!(engine.shutdown(Duration::from_secs(1)).await, Err(error));

    let mut saw_fatal = false;
    while let Ok(event) = events.try_recv() {
        if matches!(event, Event::Fatal { .. }) {
            saw_fatal = true;
        }
    }
    assert!(saw_fatal, "a Fatal event must be published");
    cloud.finish().await;
}

#[tokio::test]
async fn too_many_tunnels_retries_and_later_success_establishes() {
    let mut cloud =
        ScriptedCloud::start(vec![HandshakeResult::TooManyTunnels, HandshakeResult::Ok]).await;
    let engine = engine(cloud.address);
    let mut events = engine.events();
    let ready = AbortOnDropHandle::new(tokio::spawn({
        let engine = engine.clone();
        async move { engine.ready().await }
    }));

    let first = cloud.observation().await;
    let first_connection_id = first.credentials["tunnel-connection-id"].clone();
    assert_start_response(&first);
    assert!(
        !ready.is_finished(),
        "a retryable Cloud status terminated ready"
    );

    let second = cloud.observation().await;
    assert_start_response(&second);
    assert_ne!(
        first_connection_id, second.credentials["tunnel-connection-id"],
        "each redial must use a fresh connection ID"
    );

    let info = tokio::time::timeout(TEST_TIMEOUT, ready)
        .await
        .expect("successful retry did not establish")
        .expect("ready task panicked")
        .expect("successful trailers returned an error");
    assert_eq!(info.name, TUNNEL_NAME);

    let mut saw_failure = false;
    while let Ok(event) = events.try_recv() {
        if let Event::AttemptFailed { reason, .. } = event {
            assert!(reason.contains("too-many-tunnels"), "{reason}");
            saw_failure = true;
        }
    }
    assert!(saw_failure, "an AttemptFailed event must be published");

    engine.shutdown(Duration::ZERO).await.unwrap();
    cloud.finish().await;
}

#[tokio::test]
async fn unexpected_handshake_data_is_rejected_without_collection_and_retried() {
    let mut cloud =
        ScriptedCloud::start(vec![HandshakeResult::UnexpectedData, HandshakeResult::Ok]).await;
    let engine = engine(cloud.address);
    let ready = AbortOnDropHandle::new(tokio::spawn({
        let engine = engine.clone();
        async move { engine.ready().await }
    }));

    let first = cloud.observation().await;
    let first_connection_id = first.credentials["tunnel-connection-id"].clone();
    assert_start_response(&first);

    let second = cloud.observation().await;
    assert_start_response(&second);
    assert_ne!(
        first_connection_id, second.credentials["tunnel-connection-id"],
        "unexpected handshake DATA must reject the attempt and redial"
    );

    tokio::time::timeout(TEST_TIMEOUT, ready)
        .await
        .expect("retry after unexpected DATA did not establish")
        .expect("ready task panicked")
        .expect("successful trailers returned an error");
    engine.shutdown(Duration::ZERO).await.unwrap();
    cloud.finish().await;
}

// ───────────────────────────── reconnect policy ─────────────────────────────

/// A port nothing listens on.
async fn dead_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

#[tokio::test]
async fn giving_up_on_the_last_server_is_fatal() {
    let dead = dead_address().await;
    let options = options_for(
        vec![format!("http://{dead}")],
        Tuning {
            reconnect: ReconnectPolicy {
                max_attempts: Some(2),
                ..ReconnectPolicy::default()
            },
            ..Tuning::default()
        },
    );
    let engine = TunnelEngine::start(TestService::plain().into_service(), options).unwrap();
    let mut events = engine.events();

    let error = tokio::time::timeout(TEST_TIMEOUT, engine.ready())
        .await
        .expect("exhausted reconnect policy did not terminate ready")
        .expect_err("nothing listens on the dead port");
    match &error {
        Error::Fatal { reason } => assert!(reason.contains("max_attempts"), "{reason}"),
        other => panic!("unexpected error {other:?}"),
    }

    let mut failures = 0;
    while let Ok(event) = events.try_recv() {
        if matches!(event, Event::AttemptFailed { .. }) {
            failures += 1;
        }
    }
    assert_eq!(
        failures, 1,
        "the second attempt gives up instead of scheduling a retry"
    );
}

#[tokio::test]
async fn giving_up_on_one_server_keeps_the_others_serving() {
    let dead = dead_address().await;
    let mut cloud = FakeCloud::start().await;
    let options = options_for(
        vec![
            format!("http://{}", cloud.address),
            format!("http://{dead}"),
        ],
        Tuning {
            reconnect: ReconnectPolicy {
                // The second attempt on the dead server happens once the live
                // server is established; giving up then must not be fatal.
                initial_interval: Duration::from_millis(300),
                max_attempts: Some(2),
                max_interval: Duration::from_secs(3600),
                ..ReconnectPolicy::default()
            },
            ..Tuning::default()
        },
    );
    let engine = TunnelEngine::start(TestService::plain().into_service(), options).unwrap();
    let mut events = engine.events();
    let (engine, _) = established_for(&mut cloud, engine).await;

    let gave_up = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match events.recv().await.unwrap() {
                Event::GaveUp { target, .. } => break target,
                Event::Fatal { reason } => panic!("fatal: {reason}"),
                _ => {}
            }
        }
    })
    .await
    .expect("the dead server was not given up on");
    assert_eq!(gave_up, dead.to_string());

    // The live connection keeps serving and the engine is not terminated.
    assert_eq!(
        cloud.get("/_/health", HeaderMap::new()).await.status,
        StatusCode::OK
    );
    assert!(matches!(*engine.status().borrow(), Status::Ready(_)));

    let close = engine.shutdown(Duration::ZERO);
    cloud.wait_for_disconnect().await;
    close.await.unwrap();
}

// ───────────────────────────── server drain handover ─────────────────────────────

/// One Cloud-side HTTP/2 session. The driver task is always aborted if a test
/// assertion unwinds before the explicit close checks have reaped it.
struct CloudSession {
    sender: CloudSender,
    driver: AbortOnDropHandle<Result<(), hyper::Error>>,
    connection_id: String,
    trailers: Option<oneshot::Sender<HeaderMap>>,
}

impl CloudSession {
    async fn accept(listener: &TcpListener) -> Self {
        let (socket, _) = tokio::time::timeout(TEST_TIMEOUT, listener.accept())
            .await
            .expect("SDK did not open the replacement tunnel promptly")
            .expect("failed to accept tunnel socket");
        let (mut sender, connection) = hyper::client::conn::http2::handshake::<_, _, CloudBody>(
            TokioExecutor::new(),
            TokioIo::new(socket),
        )
        .await
        .expect("failed to start the Cloud side of HTTP/2");
        let driver = AbortOnDropHandle::new(tokio::spawn(connection));

        let (trailers, body) = handshake_body();
        let response = sender
            .send_request(
                Request::builder()
                    .method(Method::GET)
                    .uri("http://fake-tunnel.test/_/start-tunnel")
                    .body(body)
                    .unwrap(),
            )
            .await
            .expect("SDK did not answer /_/start-tunnel");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["supports-drain"], "true");
        let connection_id = response.headers()["tunnel-connection-id"]
            .to_str()
            .expect("connection id is not a header-safe string")
            .to_owned();
        uuid::Uuid::parse_str(&connection_id).expect("connection id must be a UUID");

        Self {
            sender,
            driver,
            connection_id,
            trailers: Some(trailers),
        }
    }

    fn establish(&mut self) {
        self.trailers
            .take()
            .expect("session established once")
            .send(ok_trailers())
            .expect("SDK dropped the handshake body before trailers");
    }

    async fn control(&mut self, path: &'static str) -> StatusCode {
        let response = self
            .sender
            .send_request(cloud_request(path, HeaderMap::new()))
            .await
            .expect("control request failed on a serving tunnel session");
        let status = response.status();
        response
            .into_body()
            .collect()
            .await
            .expect("control response body failed");
        status
    }

    async fn wait_closed(&mut self) {
        let result = tokio::time::timeout(TEST_TIMEOUT, &mut self.driver)
            .await
            .expect("tunnel session task did not terminate after close")
            .expect("Cloud HTTP/2 driver task panicked");
        // An abrupt client close may be reported as either a clean EOF or an
        // H2 transport error. Completion, rather than its result, proves the
        // owned driver and socket are gone.
        let _ = result;
    }
}

#[tokio::test]
async fn server_drain_overlaps_replacement_then_close_reaps_both_sessions() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let engine = engine(address);
    let mut events = engine.events();

    let mut first = CloudSession::accept(&listener).await;
    first.establish();
    tokio::time::timeout(TEST_TIMEOUT, engine.ready())
        .await
        .expect("ready did not observe the first successful handshake")
        .expect("ready failed");

    assert_eq!(first.control("/_/drain-tunnel").await, StatusCode::OK);

    // The server drain is a handover request: a replacement must start right
    // away, while the established session continues serving. Keep the second
    // handshake's trailers pending while proving the overlap.
    let mut second = CloudSession::accept(&listener).await;
    assert_ne!(first.connection_id, second.connection_id);
    assert_eq!(first.control("/_/health").await, StatusCode::OK);
    second.establish();

    let drained = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if let Event::ServerDrain { target } = events.recv().await.unwrap() {
                break target;
            }
        }
    })
    .await
    .expect("no ServerDrain event");
    assert_eq!(drained, address.to_string());

    let close = engine.shutdown(Duration::ZERO);
    let ((), (), result) = tokio::join!(first.wait_closed(), second.wait_closed(), close);
    result.expect("closing the tunnel engine failed");
}

// ───────────────────────────── start errors ─────────────────────────────

#[test]
fn start_requires_a_runtime() {
    let options = options("127.0.0.1:1".parse().unwrap());
    let error = TunnelEngine::start(TestService::plain().into_service(), options).unwrap_err();
    assert!(error.to_string().contains("tokio runtime"), "{error}");
}
