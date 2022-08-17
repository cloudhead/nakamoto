use super::Options;

impl quickcheck::Arbitrary for Options {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
        let rng = fastrand::Rng::with_seed(u64::arbitrary(g));
        let from = rng.u64(0..=1);
        let to = rng.u64(2..4);
        let failure_rate = rng.f64() / 4.;

        Self {
            latency: from..to,
            failure_rate,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let failure_rate = self.failure_rate - 0.01;
        let latency = self.latency.start.saturating_sub(1)..self.latency.end.saturating_sub(1);

        if failure_rate < 0. && latency.is_empty() {
            return Box::new(std::iter::empty());
        }

        Box::new(std::iter::once(Self {
            latency,
            failure_rate,
        }))
    }
}
