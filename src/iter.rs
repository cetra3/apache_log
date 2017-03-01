use futures::{Async, Poll};
use futures::stream::Stream;

pub struct IterStream<I> {
    iter: I,
}

pub fn iter<J, T, E>(i: J) -> IterStream<J>
    where J: Iterator<Item=Result<T, E>>,
{
    IterStream {
        iter: i,
    }
}

impl<I, T, E> Stream for IterStream<I>
    where I: Iterator<Item=Result<T, E>>,
{
    type Item = T;
    type Error = E;

    fn poll(&mut self) -> Poll<Option<T>, E> {
        match self.iter.next() {
            Some(Ok(e)) => Ok(Async::Ready(Some(e))),
            Some(Err(e)) => Err(e),
            None => Ok(Async::Ready(None)),
        }
    }
}
