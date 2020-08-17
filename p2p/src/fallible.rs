use std::sync::Mutex;

lazy_static! {
    pub(super) static ref FALLIBLE: Mutex<Option<f64>> = Mutex::new(None);
}

pub(crate) struct FailGuard {}

impl Drop for FailGuard {
    fn drop(&mut self) {
        let mut fallible = self::FALLIBLE.lock().unwrap();
        *fallible = None;
    }
}

#[allow(dead_code)]
pub(crate) fn set_fallible(p: f64) -> FailGuard {
    let mut fallible = self::FALLIBLE.lock().unwrap();
    *fallible = Some(p);

    FailGuard {}
}
