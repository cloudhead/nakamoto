#![allow(dead_code)]
use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};

struct Waker;

impl std::task::Wake for Waker {
    fn wake(self: Arc<Self>) {}
    fn wake_by_ref(self: &Arc<Self>) {}
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

struct Task {
    future: Option<BoxFuture<'static, ()>>,
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Task").finish()
    }
}

#[derive(Debug, Clone)]
pub struct Request<T> {
    result: Rc<RefCell<Option<Result<T, ()>>>>,
}

impl<T> Request<T> {
    pub fn new() -> Self {
        Self {
            result: Rc::new(RefCell::new(None)),
        }
    }

    pub fn complete(&mut self, result: Result<T, ()>) {
        *self.result.borrow_mut() = Some(result);
    }
}

impl<T: Clone + std::marker::Unpin + std::fmt::Debug> Future for Request<T> {
    type Output = Result<T, ()>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _ctx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // TODO: Use `take()` instead of cloning, once you figure it out.
        // For now we have to clone, as multiple futures may share the same
        // refcell.
        if let Some(result) = self.get_mut().result.borrow().clone() {
            Poll::Ready(result)
        } else {
            Poll::Pending
        }
    }
}

#[derive(Clone, Debug)]
pub struct Executor {
    tasks: Rc<RefCell<Vec<Task>>>,
}

impl Executor {
    pub fn new() -> Self {
        Self {
            tasks: Rc::new(RefCell::new(Vec::new())),
        }
    }

    /// Spawn a future to be executed.
    pub fn spawn(&mut self, future: impl Future<Output = ()> + 'static) {
        self.tasks.borrow_mut().push(Task {
            future: Some(Box::pin(future)),
        });
    }

    /// Poll all tasks for completion.
    pub fn poll(&mut self) -> Poll<()> {
        let mut tasks = self.tasks.borrow_mut();
        let waker = Arc::new(Waker).into();
        let mut cx = Context::from_waker(&waker);

        for task in tasks.iter_mut() {
            if let Some(mut fut) = task.future.take() {
                if fut.as_mut().poll(&mut cx).is_pending() {
                    task.future = Some(fut);
                }
            }
        }
        // Clear out all completed futures.
        tasks.retain(|t| t.future.is_some());

        if tasks.is_empty() {
            return Poll::Ready(());
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Random<T> {
        val: T,
    }

    impl<T: Clone + std::fmt::Debug + Unpin> Future for Random<T> {
        type Output = T;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            if fastrand::bool() {
                Poll::Ready(self.val.clone())
            } else {
                Poll::Pending
            }
        }
    }

    #[test]
    fn test_executor() {
        let mut exe = Executor::new();

        exe.spawn(async {
            Random { val: 1 }.await;
        });
        exe.spawn(async {
            Random { val: 2 }.await;
        });
        exe.spawn(async {
            Random { val: 3 }.await;
        });

        loop {
            if let Poll::Ready(()) = exe.poll() {
                break;
            }
        }
    }
}
