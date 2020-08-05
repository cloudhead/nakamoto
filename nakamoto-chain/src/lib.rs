#[allow(clippy::len_without_is_empty)]
#[allow(clippy::collapsible_if)]
#[deny(
    unsafe_code,
    missing_debug_implementations,
    missing_copy_implementations
)]
pub mod block;

#[cfg(test)]
mod tests;
