//! Small combinators for composing synchronous/asynchronous nusb operations.

use std::{
    future::{Future, IntoFuture},
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use nusb::MaybeFuture;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use std::marker::Send as NonWasmSend;

#[cfg(target_arch = "wasm32")]
pub(crate) trait NonWasmSend {}
#[cfg(target_arch = "wasm32")]
impl<T> NonWasmSend for T {}

pub(crate) fn ready<T: NonWasmSend>(value: T) -> impl MaybeFuture<Output = T> {
    Ready(value)
}

struct Ready<T>(T);

impl<T> IntoFuture for Ready<T> {
    type Output = T;
    type IntoFuture = std::future::Ready<T>;

    fn into_future(self) -> Self::IntoFuture {
        std::future::ready(self.0)
    }
}

impl<T: NonWasmSend> MaybeFuture for Ready<T> {
    #[cfg(not(target_arch = "wasm32"))]
    fn wait(self) -> Self::Output {
        self.0
    }
}

pub(crate) trait MaybeFutureExt: MaybeFuture + Sized {
    fn and_then<C, T, U, E, N>(self, continuation: C) -> impl MaybeFuture<Output = Result<U, E>>
    where
        Self: MaybeFuture<Output = Result<T, E>>,
        C: FnOnce(T) -> N + NonWasmSend,
        N: MaybeFuture<Output = Result<U, E>>,
    {
        AndThen {
            wrapped: self,
            continuation,
            next: PhantomData,
        }
    }
}

impl<F: MaybeFuture> MaybeFutureExt for F {}

struct AndThen<F, C, N> {
    wrapped: F,
    continuation: C,
    next: PhantomData<fn() -> N>,
}

impl<F, C, N, T, U, E> IntoFuture for AndThen<F, C, N>
where
    F: MaybeFuture<Output = Result<T, E>>,
    C: FnOnce(T) -> N + NonWasmSend,
    N: MaybeFuture<Output = Result<U, E>>,
{
    type Output = Result<U, E>;
    type IntoFuture = AndThenFuture<F::IntoFuture, C, N::IntoFuture>;

    fn into_future(self) -> Self::IntoFuture {
        AndThenFuture {
            first: Some(Box::pin(self.wrapped.into_future())),
            continuation: Some(self.continuation),
            second: None,
        }
    }
}

impl<F, C, N, T, U, E> MaybeFuture for AndThen<F, C, N>
where
    F: MaybeFuture<Output = Result<T, E>>,
    C: FnOnce(T) -> N + NonWasmSend,
    N: MaybeFuture<Output = Result<U, E>>,
{
    #[cfg(not(target_arch = "wasm32"))]
    fn wait(self) -> Self::Output {
        let value = self.wrapped.wait()?;
        (self.continuation)(value).wait()
    }
}

struct AndThenFuture<F, C, N> {
    first: Option<Pin<Box<F>>>,
    continuation: Option<C>,
    second: Option<Pin<Box<N>>>,
}

impl<F, C, N> Unpin for AndThenFuture<F, C, N> {}

impl<F, C, N, Next, T, U, E> Future for AndThenFuture<F, C, N>
where
    F: Future<Output = Result<T, E>>,
    C: FnOnce(T) -> Next,
    Next: IntoFuture<Output = Result<U, E>, IntoFuture = N>,
    N: Future<Output = Result<U, E>>,
{
    type Output = Result<U, E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            if let Some(second) = self.second.as_mut() {
                return second.as_mut().poll(cx);
            }
            let first = self.first.as_mut().expect("polled after completion");
            match first.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    self.first = None;
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Ok(value)) => {
                    self.first = None;
                    let continuation = self.continuation.take().expect("missing continuation");
                    self.second = Some(Box::pin(continuation(value).into_future()));
                }
            }
        }
    }
}

pub(crate) enum Either<L, R> {
    Left(L),
    Right(R),
}

impl<L, R> Either<L, R> {
    pub(crate) fn left(value: L) -> Self {
        Self::Left(value)
    }

    pub(crate) fn right(value: R) -> Self {
        Self::Right(value)
    }
}

impl<L, R> IntoFuture for Either<L, R>
where
    L: MaybeFuture,
    R: MaybeFuture<Output = L::Output>,
{
    type Output = L::Output;
    type IntoFuture = EitherFuture<L::IntoFuture, R::IntoFuture>;

    fn into_future(self) -> Self::IntoFuture {
        match self {
            Self::Left(future) => EitherFuture::Left(Box::pin(future.into_future())),
            Self::Right(future) => EitherFuture::Right(Box::pin(future.into_future())),
        }
    }
}

impl<L, R> MaybeFuture for Either<L, R>
where
    L: MaybeFuture,
    R: MaybeFuture<Output = L::Output>,
{
    #[cfg(not(target_arch = "wasm32"))]
    fn wait(self) -> Self::Output {
        match self {
            Self::Left(future) => future.wait(),
            Self::Right(future) => future.wait(),
        }
    }
}

pub(crate) enum EitherFuture<L, R> {
    Left(Pin<Box<L>>),
    Right(Pin<Box<R>>),
}

impl<L, R> Unpin for EitherFuture<L, R> {}

impl<L, R> Future for EitherFuture<L, R>
where
    L: Future,
    R: Future<Output = L::Output>,
{
    type Output = L::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut *self {
            Self::Left(future) => future.as_mut().poll(cx),
            Self::Right(future) => future.as_mut().poll(cx),
        }
    }
}
