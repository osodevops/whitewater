use std::fmt;

use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use super::{RangeGeneration, RangeId};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyToken([u8; 16]);

impl KeyToken {
    pub const MIN: Self = Self([0; 16]);

    pub fn from_key(key: &[u8]) -> Self {
        let digest = blake3::hash(key);
        let mut value = [0; 16];
        value.copy_from_slice(&digest.as_bytes()[..16]);
        Self(value)
    }

    pub const fn from_bytes(value: [u8; 16]) -> Self {
        Self(value)
    }

    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for KeyToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for KeyToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl Serialize for KeyToken {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for KeyToken {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value.len() != 32 {
            return Err(D::Error::custom(
                "KeyToken must contain 32 hexadecimal characters",
            ));
        }
        let mut bytes = [0_u8; 16];
        for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
            let text = std::str::from_utf8(chunk).map_err(D::Error::custom)?;
            bytes[index] = u8::from_str_radix(text, 16).map_err(D::Error::custom)?;
        }
        Ok(Self(bytes))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct KeyRange {
    pub start: KeyToken,
    pub end_exclusive: Option<KeyToken>,
}

impl KeyRange {
    pub const fn full() -> Self {
        Self {
            start: KeyToken::MIN,
            end_exclusive: None,
        }
    }

    pub fn contains(&self, token: KeyToken) -> bool {
        token >= self.start && self.end_exclusive.is_none_or(|end| token < end)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeRoute {
    pub range_id: RangeId,
    pub generation: RangeGeneration,
    pub bounds: KeyRange,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct RangeMap {
    routes: Vec<RangeRoute>,
}

impl<'de> Deserialize<'de> for RangeMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Representation {
            routes: Vec<RangeRoute>,
        }
        Self::try_new(Representation::deserialize(deserializer)?.routes).map_err(D::Error::custom)
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum RangeMapError {
    #[error("range map must contain at least one range")]
    Empty,
    #[error("range map must start at the beginning of the logical keyspace")]
    MissingStart,
    #[error("range map has a gap or overlap at route index {index}")]
    NonContiguous { index: usize },
    #[error("range at index {index} is empty or reversed")]
    InvalidBounds { index: usize },
    #[error("range map must end at the end of the logical keyspace")]
    MissingEnd,
    #[error("split token is outside the selected range or lies on its boundary")]
    InvalidSplit,
    #[error("ranges selected for merge are missing, identical, or not adjacent in map order")]
    InvalidMerge,
    #[error("range generation overflow")]
    GenerationOverflow,
}

impl RangeMap {
    pub fn single(range_id: RangeId, generation: RangeGeneration) -> Self {
        Self {
            routes: vec![RangeRoute {
                range_id,
                generation,
                bounds: KeyRange::full(),
            }],
        }
    }

    pub fn try_new(routes: Vec<RangeRoute>) -> Result<Self, RangeMapError> {
        if routes.is_empty() {
            return Err(RangeMapError::Empty);
        }
        if routes[0].bounds.start != KeyToken::MIN {
            return Err(RangeMapError::MissingStart);
        }
        for (index, route) in routes.iter().enumerate() {
            if route
                .bounds
                .end_exclusive
                .is_some_and(|end| end <= route.bounds.start)
            {
                return Err(RangeMapError::InvalidBounds { index });
            }
            if index > 0 && routes[index - 1].bounds.end_exclusive != Some(route.bounds.start) {
                return Err(RangeMapError::NonContiguous { index });
            }
            if index + 1 < routes.len() && route.bounds.end_exclusive.is_none() {
                return Err(RangeMapError::NonContiguous { index: index + 1 });
            }
        }
        if routes
            .last()
            .and_then(|route| route.bounds.end_exclusive)
            .is_some()
        {
            return Err(RangeMapError::MissingEnd);
        }
        Ok(Self { routes })
    }

    pub fn routes(&self) -> &[RangeRoute] {
        &self.routes
    }

    pub fn route_key(&self, key: &[u8]) -> &RangeRoute {
        self.route_token(KeyToken::from_key(key))
    }

    pub fn route_token(&self, token: KeyToken) -> &RangeRoute {
        let index = self
            .routes
            .partition_point(|route| route.bounds.start <= token);
        &self.routes[index.saturating_sub(1)]
    }

    pub fn split(
        &self,
        range_id: RangeId,
        split_at: KeyToken,
        right_range_id: RangeId,
    ) -> Result<Self, RangeMapError> {
        let index = self
            .routes
            .iter()
            .position(|route| route.range_id == range_id)
            .ok_or(RangeMapError::InvalidSplit)?;
        let route = &self.routes[index];
        if split_at <= route.bounds.start
            || route
                .bounds
                .end_exclusive
                .is_some_and(|end| split_at >= end)
        {
            return Err(RangeMapError::InvalidSplit);
        }
        let generation = route
            .generation
            .checked_next()
            .map_err(|_| RangeMapError::GenerationOverflow)?;
        let mut routes = self.routes.clone();
        routes.splice(
            index..=index,
            [
                RangeRoute {
                    range_id,
                    generation,
                    bounds: KeyRange {
                        start: route.bounds.start,
                        end_exclusive: Some(split_at),
                    },
                },
                RangeRoute {
                    range_id: right_range_id,
                    generation,
                    bounds: KeyRange {
                        start: split_at,
                        end_exclusive: route.bounds.end_exclusive,
                    },
                },
            ],
        );
        Self::try_new(routes)
    }

    pub fn merge_adjacent(
        &self,
        left_range_id: RangeId,
        right_range_id: RangeId,
    ) -> Result<Self, RangeMapError> {
        let left_index = self
            .routes
            .iter()
            .position(|route| route.range_id == left_range_id)
            .ok_or(RangeMapError::InvalidMerge)?;
        if left_index + 1 >= self.routes.len()
            || self.routes[left_index + 1].range_id != right_range_id
        {
            return Err(RangeMapError::InvalidMerge);
        }
        let left = &self.routes[left_index];
        let right = &self.routes[left_index + 1];
        if left.bounds.end_exclusive != Some(right.bounds.start) {
            return Err(RangeMapError::InvalidMerge);
        }
        let generation = left
            .generation
            .max(right.generation)
            .checked_next()
            .map_err(|_| RangeMapError::GenerationOverflow)?;
        let mut routes = self.routes.clone();
        routes.splice(
            left_index..=left_index + 1,
            [RangeRoute {
                range_id: left_range_id,
                generation,
                bounds: KeyRange {
                    start: left.bounds.start,
                    end_exclusive: right.bounds.end_exclusive,
                },
            }],
        );
        Self::try_new(routes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn range(value: u128) -> RangeId {
        RangeId::from_uuid(Uuid::from_u128(value))
    }

    #[test]
    fn stable_hash_routes_the_same_key_to_the_same_range() {
        let map = RangeMap::single(range(1), RangeGeneration::new(1));
        assert_eq!(map.route_key(b"account-123"), map.route_key(b"account-123"));
        assert_eq!(KeyToken::from_key(b"account-123").to_string().len(), 32);
    }

    #[test]
    fn split_is_half_open_contiguous_and_preserves_total_coverage() {
        let original = RangeMap::single(range(1), RangeGeneration::new(1));
        let split = KeyToken::from_bytes([0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let map = original.split(range(1), split, range(2)).unwrap();
        assert_eq!(map.routes().len(), 2);
        assert_eq!(map.route_token(KeyToken::MIN).range_id, range(1));
        assert_eq!(map.route_token(split).range_id, range(2));
        assert_eq!(map.routes()[0].bounds.end_exclusive, Some(split));
        assert_eq!(map.routes()[1].bounds.end_exclusive, None);
        assert_eq!(map.routes()[0].generation, RangeGeneration::new(2));
    }

    #[test]
    fn invalid_gaps_overlaps_and_boundary_splits_are_rejected() {
        let middle = KeyToken::from_bytes([0x80; 16]);
        let gap = RangeMap::try_new(vec![
            RangeRoute {
                range_id: range(1),
                generation: RangeGeneration::new(1),
                bounds: KeyRange {
                    start: KeyToken::MIN,
                    end_exclusive: Some(middle),
                },
            },
            RangeRoute {
                range_id: range(2),
                generation: RangeGeneration::new(1),
                bounds: KeyRange {
                    start: KeyToken::from_bytes([0x81; 16]),
                    end_exclusive: None,
                },
            },
        ]);
        assert!(matches!(gap, Err(RangeMapError::NonContiguous { .. })));
        let map = RangeMap::single(range(1), RangeGeneration::new(1));
        assert_eq!(
            map.split(range(1), KeyToken::MIN, range(2)),
            Err(RangeMapError::InvalidSplit)
        );
    }

    #[test]
    fn adjacent_ranges_merge_without_gaps_and_advance_generation() {
        let split_at = KeyToken::from_bytes([0x80; 16]);
        let split = RangeMap::single(range(1), RangeGeneration::new(1))
            .split(range(1), split_at, range(2))
            .unwrap();
        let merged = split.merge_adjacent(range(1), range(2)).unwrap();
        assert_eq!(merged.routes().len(), 1);
        assert_eq!(merged.routes()[0].range_id, range(1));
        assert_eq!(merged.routes()[0].generation, RangeGeneration::new(3));
        assert_eq!(merged.routes()[0].bounds, KeyRange::full());
        assert_eq!(
            split.merge_adjacent(range(2), range(1)),
            Err(RangeMapError::InvalidMerge)
        );
    }

    #[test]
    fn key_tokens_round_trip_as_portable_hex_strings() {
        let token = KeyToken::from_key(b"customer-7");
        let encoded = serde_json::to_string(&token).unwrap();
        assert_eq!(serde_json::from_str::<KeyToken>(&encoded).unwrap(), token);
    }
}
