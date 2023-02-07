/// Unfinished attempt at building etag middleware for Tower
struct EtagMiddleware<S> {
    inner: S,
    request_etag: Option<EntityTag>,
}

impl<S> Service<Request<Body>> for EtagMiddleware<S>
where
    S: Service<Vec<u8>>,
    S::Error: Into<BoxError>,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: Request<Vec<u8>>) -> Self::Future {
        let if_none_match_header = request.headers().get(header::IF_NONE_MATCH).cloned();
        match if_none_match_header {
            Some(if_none_match_header) => {
                let fut = async {
                    let response = self.inner.call(request).await;
                    match response {
                        Ok(resopnse) => {}
                        Err(_) => todo!(),
                    }
                };
                Box::pin(fut)
            }
            None => {
                self.inner.call(request);
            }
        }
    }
}
