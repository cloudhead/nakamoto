//! Double-ended iterator over a `NonEmpty` blockchain.
use nonempty::NonEmpty;

use super::Height;

/// An iterator over a chain.
#[derive(Debug)]
pub struct Iter<'a, T> {
    chain: &'a NonEmpty<T>,
    next: usize,
    next_back: usize,
}

impl<'a, T> Iter<'a, T> {
    /// Create a new iterator.
    pub fn new(chain: &'a NonEmpty<T>) -> Self {
        Self {
            chain,
            next: 0,
            next_back: chain.len(),
        }
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = (Height, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.next_back {
            return None;
        }
        let height = self.next;
        self.next += 1;

        self.chain.get(height).map(|item| (height as Height, item))
    }
}

impl<'a, T> DoubleEndedIterator for Iter<'a, T> {
    ///```
    /// use nonempty::NonEmpty;
    /// use nakamoto_common::block::iter::Iter;
    ///
    /// let chain = NonEmpty::from_vec(vec![1, 2, 3, 4, 5]).unwrap();
    /// let mut iter = Iter::new(&chain);
    ///
    /// assert_eq!(Some((4, &5)), iter.next_back());
    /// assert_eq!(Some((3, &4)), iter.next_back());
    /// assert_eq!(Some((2, &3)), iter.next_back());
    /// assert_eq!(Some((1, &2)), iter.next_back());
    /// assert_eq!(Some((0, &1)), iter.next_back());
    ///
    /// let mut iter = Iter::new(&chain);
    ///
    /// assert_eq!(Some((4, &5)), iter.next_back());
    /// assert_eq!(Some((0, &1)), iter.next());
    /// assert_eq!(Some((3, &4)), iter.next_back());
    /// assert_eq!(Some((1, &2)), iter.next());
    /// assert_eq!(Some((2, &3)), iter.next_back());
    /// assert_eq!(None, iter.next());
    /// assert_eq!(None, iter.next_back());
    ///```
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.next_back == self.next {
            return None;
        }

        self.next_back -= 1;
        let height = self.next_back;

        self.chain.get(height).map(|item| (height as Height, item))
    }
}
