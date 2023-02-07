use axum::{
    body::HttpBody,
    http::{header, HeaderValue, Request},
    middleware::Next,
    response::{IntoResponse, Response},
};
use bytes::BufMut;
use etag::EntityTag;
use reqwest::StatusCode;
use tracing::{error, trace};

pub async fn middleware_fn<B: std::fmt::Debug>(
    req: Request<B>,
    next: Next<B>,
) -> Result<Response, StatusCode> {
    let if_none_match_header = req.headers().get(header::IF_NONE_MATCH).cloned();
    let path = req.uri().path().to_owned();
    let res = next.run(req).await;
    let (mut parts, mut body) = res.into_parts();

    let bytes = {
        let mut body_bytes = vec![];

        while let Some(inner) = body.data().await {
            let bytes = inner.unwrap();
            body_bytes.put(bytes);
        }

        body_bytes
    };

    match bytes.is_empty() {
        true => {
            trace!(path, "response without body, skipping etag");
            Ok(parts.into_response())
        }
        false => match if_none_match_header {
            None => {
                let etag = EntityTag::from_data(&bytes);

                parts.headers.insert(
                    header::ETAG,
                    HeaderValue::from_str(&etag.to_string()).unwrap(),
                );

                trace!(path, %etag, "no if-none-match header");

                Ok((parts, bytes).into_response())
            }
            Some(if_none_match) => {
                let if_none_match_etag = if_none_match.to_str().unwrap().parse::<EntityTag>();
                match if_none_match_etag {
                    Err(ref err) => {
                        error!("{} - {:?}", err, &if_none_match_etag);
                        let etag = EntityTag::from_data(&bytes);
                        parts.headers.insert(
                            header::ETAG,
                            HeaderValue::from_str(&etag.to_string()).unwrap(),
                        );
                        Ok((parts, bytes).into_response())
                    }
                    Ok(if_none_match_etag) => {
                        let etag = EntityTag::from_data(&bytes);

                        parts.headers.insert(
                            header::ETAG,
                            HeaderValue::from_str(&etag.to_string()).unwrap(),
                        );

                        let some_match = etag.strong_eq(&if_none_match_etag);

                        trace!(
                            path,
                            %etag,
                            some_match,
                            "if-none-match" = %if_none_match_etag
                        );

                        if some_match {
                            Ok((StatusCode::NOT_MODIFIED, parts).into_response())
                        } else {
                            Ok((parts, bytes).into_response())
                        }
                    }
                }
            }
        },
    }
}
