// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use pin_project::pin_project;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tower::{Layer, Service};

/// Tracks the number of in-flight requests being processed by a service and rejects new incoming
/// requests if the number of in-flight requests exceeds a specified limit.
#[derive(Debug)]
pub struct LoadShed<S> {
    inner: S,
    permits: Arc<Semaphore>,
    permit_opt: Option<OwnedSemaphorePermit>,
}

impl<S> Clone for LoadShed<S>
where S: Clone
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            permits: self.permits.clone(),
            permit_opt: None,
        }
    }
}

pub trait MakeLoadShedError {
    fn make_load_shed_error() -> Self;
}

impl<S, R> Service<R> for LoadShed<S>
where
    S: Service<R>,
    S::Error: MakeLoadShedError,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = LoadShedFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.permit_opt.is_none() {
            if let Ok(permit) = self.permits.clone().try_acquire_owned() {
                self.permit_opt = Some(permit);
            } else {
                return Poll::Ready(Err(S::Error::make_load_shed_error()));
            }
        }
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: R) -> Self::Future {
        let permit = self
            .permit_opt
            .take()
            .expect("`poll_ready` should be called before `call`");

        LoadShedFuture {
            inner: self.inner.call(request),
            permit,
        }
    }
}

#[pin_project]
#[derive(Debug)]
pub struct LoadShedFuture<F> {
    #[pin]
    inner: F,
    permit: OwnedSemaphorePermit,
}

impl<F, T, E> Future for LoadShedFuture<F>
where F: Future<Output = Result<T, E>>
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

/// Allows at most `max_in_flight_requests` in-flight requests before rejecting new incoming
/// requests.
#[derive(Debug, Clone)]
pub struct LoadShedLayer {
    max_in_flight_requests: usize,
}

impl LoadShedLayer {
    /// Creates a new `LoadShedLayer` allowing at most `max_in_flight_requests` in-flight requests
    /// before rejecting new incoming requests.
    pub fn new(max_in_flight_requests: usize) -> Self {
        Self {
            max_in_flight_requests,
        }
    }
}

impl<S> Layer<S> for LoadShedLayer {
    type Service = LoadShed<S>;

    fn layer(&self, service: S) -> Self::Service {
        LoadShed {
            inner: service,
            permits: Arc::new(Semaphore::new(self.max_in_flight_requests)),
            permit_opt: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use tower::{ServiceBuilder, ServiceExt};

    use super::*;

    #[tokio::test]
    async fn test_load_shed() {
        #[derive(Debug)]
        struct MyError;

        impl MakeLoadShedError for MyError {
            fn make_load_shed_error() -> Self {
                MyError
            }
        }
        let mut service = ServiceBuilder::new()
            .layer(LoadShedLayer::new(1))
            .service_fn(|_| async { Ok::<_, MyError>(()) });

        let in_fight_fut = service.ready().await.unwrap().call(());
        service.ready().await.unwrap_err();

        drop(in_fight_fut);
        service.ready().await.unwrap().call(()).await.unwrap();
    }
}
