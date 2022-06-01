use async_trait::async_trait;
use hyper::{self, header, Body, Method, Request, Response, Server, StatusCode};
use tikv::server::status_server;

#[derive(Clone)]
pub struct TiFlashEngineStatus {
    engine_store_server_helper: &'static crate::EngineStoreServerHelper,
}

impl TiFlashEngineStatus {
    pub fn new(engine_store_server_helper: &'static crate::EngineStoreServerHelper) -> Self {
        TiFlashEngineStatus {
            engine_store_server_helper,
        }
    }
}

#[async_trait]
impl status_server::EngineStatusHandler for TiFlashEngineStatus {
    async fn handle(&self, req: Request<Body>) -> hyper::Result<hyper::Response<Body>> {
        handle_http_request(req, self.engine_store_server_helper).await
    }
}

pub async fn handle_http_request(
    req: Request<Body>,
    engine_store_server_helper: &'static crate::EngineStoreServerHelper,
) -> hyper::Result<Response<Body>> {
    let (head, body) = req.into_parts();
    let body = hyper::body::to_bytes(body).await;

    match body {
        Ok(s) => {
            let res = engine_store_server_helper.handle_http_request(
                head.uri.path(),
                head.uri.query(),
                &s,
            );
            if res.status != crate::HttpRequestStatus::Ok {
                return Ok(err_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "engine-store fails to build response".to_string(),
                ));
            }

            let data = res.res.view.to_slice().to_vec();

            match Response::builder().body(hyper::Body::from(data)) {
                Ok(resp) => Ok(resp),
                Err(err) => Ok(err_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("fails to build response: {}", err),
                )),
            }
        }
        Err(err) => Ok(err_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("fails to build response: {}", err),
        )),
    }
}

fn err_response<T>(status_code: StatusCode, message: T) -> Response<Body>
where
    T: Into<Body>,
{
    Response::builder()
        .status(status_code)
        .body(message.into())
        .unwrap()
}
