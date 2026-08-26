use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Invalid ord range: ({lower}, {upper})")]
    InvalidRange { lower: u64, upper: u64 },

    #[error("Invalid range split: ({range:?}, {size})")]
    InvalidSplit { range: OrdinalRange, size: u64 },

    #[error("Invalid take amount: ({ranges:?}, {take})")]
    InvalidTake { ranges: OrdinalRanges, take: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Inclusive range of ordinals
pub struct OrdinalRange {
    lower: u64,
    upper: u64,
}

impl OrdinalRange {
    pub fn new(lower: u64, upper: u64) -> Result<Self, Error> {
        if upper < lower {
            Err(Error::InvalidRange { lower, upper })
        } else {
            Ok(Self { lower, upper })
        }
    }

    pub fn new_coinbase_ords(height: u64) -> Result<Self, Error> {
        fn subsidy(height: u64) -> u64 {
            (50 * 100_000_000) >> (height / 210_000)
        }

        let first_ordinal = {
            let mut start = 0;

            for h in 0..height {
                start += subsidy(h);
            }

            start
        };

        // inclusive range
        let last_ordinal = first_ordinal + subsidy(height) - 1;

        OrdinalRange::new(first_ordinal, last_ordinal)
    }

    pub fn lower(&self) -> u64 {
        self.lower
    }

    pub fn upper(&self) -> u64 {
        self.upper
    }

    pub fn size(&self) -> u64 {
        self.upper + 1 - self.lower
    }

    pub fn split(self, size: u64) -> Result<(Self, Self), Error> {
        let len = self.size();

        if size == 0 || size >= len {
            return Err(Error::InvalidSplit { range: self, size });
        }

        let Self { lower, upper } = self;

        let left = (lower, (lower + size - 1));
        let right = (lower + size, upper);

        Ok((
            OrdinalRange::new(left.0, left.1)?,
            OrdinalRange::new(right.0, right.1)?,
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrdinalRanges(VecDeque<OrdinalRange>);

impl OrdinalRanges {
    pub fn new() -> Self {
        Self(VecDeque::new())
    }

    pub fn push(&mut self, range: OrdinalRange) {
        self.0.push_back(range)
    }

    pub fn extend(&mut self, ranges: OrdinalRanges) {
        self.0.extend(ranges.0)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn take(&mut self, size: u64) -> Result<OrdinalRanges, Error> {
        let mut require = size;
        let mut out = OrdinalRanges::new();

        while require > 0 {
            let front = self.0.pop_front().ok_or_else(|| Error::InvalidTake {
                ranges: self.clone(),
                take: size,
            })?;

            if front.size() > require {
                let (left, right) = front.split(require)?;
                out.push(left);
                self.0.push_front(right);
                break;
            }

            require -= front.size();
            out.push(front)
        }

        Ok(out)
    }

    pub fn to_vec(self) -> Vec<OrdinalRange> {
        self.0.into()
    }
}

#[cfg(test)]
mod tests {
    use super::{OrdinalRange, OrdinalRanges};

    #[test]
    fn it_works() {
        let range1 = OrdinalRange::new(12, 15).unwrap();
        let range2 = OrdinalRange::new(3, 7).unwrap();
        let range3 = OrdinalRange::new(1, 1).unwrap();

        let mut ranges = OrdinalRanges::new();

        ranges.push(range1);
        ranges.push(range2);
        ranges.push(range3);

        let took = ranges.take(6).unwrap();

        println!("{:?} {:?}", took, ranges);
    }

    #[test]
    fn coinbase_ord_works() {
        let range1 = OrdinalRange::new_coinbase_ords(0);

        println!("{:?}", range1);
    }
}
