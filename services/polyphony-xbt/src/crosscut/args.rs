use bitcoin::BlockHash;
use bitcoin::hashes::Hash;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::{ops::Deref, str::FromStr};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct Point {
    pub height: u64,
    pub hash: BlockHash,
}

impl Default for Point {
    fn default() -> Self {
        Self {
            height: Default::default(),
            hash: BlockHash::from_byte_array([0; 32]),
        }
    }
}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.height, self.hash)
    }
}

/// A serialization-friendly chain Point struct using a hex-encoded hash
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PointArg {
    Origin,
    Specific(u64, String),
}

impl From<Point> for PointArg {
    fn from(other: Point) -> Self {
        PointArg::Specific(other.height, other.hash.to_string())
    }
}

impl FromStr for PointArg {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            x if s.contains(',') => {
                let mut parts: Vec<_> = x.split(',').collect();
                let slot = parts
                    .remove(0)
                    .parse()
                    .map_err(|_| Self::Err::message("can't parse slot number"))?;

                let hash = parts.remove(0).to_owned();
                Ok(PointArg::Specific(slot, hash))
            }
            "origin" => Ok(PointArg::Origin),
            _ => Err(Self::Err::message(
                "Can't parse chain point value, expecting `slot,hex-hash` format",
            )),
        }
    }
}

impl ToString for PointArg {
    fn to_string(&self) -> String {
        match self {
            PointArg::Origin => "origin".to_string(),
            PointArg::Specific(slot, hash) => format!("{},{}", slot, hash),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct MagicArg(pub u64);

impl Deref for MagicArg {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromStr for MagicArg {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let m = match s {
            // "testnet" => MagicArg(TESTNET_MAGIC),
            // "mainnet" => MagicArg(MAINNET_MAGIC),
            // "preview" => MagicArg(PREVIEW_MAGIC),
            // "preprod" => MagicArg(PRE_PRODUCTION_MAGIC),
            _ => MagicArg(u64::from_str(s).map_err(|_| "can't parse magic value")?),
        };

        Ok(m)
    }
}

// impl Default for MagicArg {
//     fn default() -> Self {
//         // Self(MAINNET_MAGIC)
//     }
// }

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", content = "value")]
pub enum IntersectConfig {
    Tip,
    Origin,
    Point(u64, String),
}

impl IntersectConfig {
    pub fn get_point(&self) -> Option<Point> {
        match self {
            IntersectConfig::Point(height, hash) => Some(Point {
                height: *height,
                hash: BlockHash::from_str(hash).expect("valid block hash"),
            }),
            _ => None,
        }
    }
}
