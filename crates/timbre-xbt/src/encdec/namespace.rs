use std::ops::Range;

use crate::{
    prefix_key_range, rollback, CollectionIngestor, Decode, MinerIngestor, Reducer, TimbreError,
};

use super::{enc, Encode};

#[derive(Debug, Clone, PartialEq)]
pub struct Namespace {
    pub dataplane_id: u8,
    pub instance_id: u16,
}

impl Namespace {
    pub fn new(dataplane_id: u8, instance_id: u16) -> Self {
        Self {
            dataplane_id,
            instance_id,
        }
    }

    pub fn dataplane_key_range(&self) -> Range<Vec<u8>> {
        prefix_key_range(&[self.dataplane_id])
    }

    pub fn instance_key_range(&self) -> Range<Vec<u8>> {
        prefix_key_range(&self.encode())
    }

    pub fn batch_complete_key_range(&self) -> Range<Vec<u8>> {
        let prefix = enc().append(self).batch_complete_tag().break_().build();

        prefix_key_range(&prefix)
    }

    // dataplane (u8) + instance (u16)
    pub fn size() -> usize {
        size_of::<u8>() + size_of::<u16>()
    }
}

impl Encode for Namespace {
    fn encode(&self) -> Vec<u8> {
        enc()
            .append(&self.dataplane_id.to_be_bytes())
            .append(&self.instance_id.to_be_bytes())
            .build()
    }
}

impl Decode for Namespace {
    fn decode(bytes: &[u8]) -> Result<(Self, &[u8]), TimbreError> {
        if bytes.len() < Namespace::size() {
            return Err(TimbreError::MalformedInput(
                "Insufficient bytes for Namespace decode".into(),
            ));
        }

        let dataplane_id = bytes[0];
        let instance_id = u16::from_be_bytes(bytes[1..3].try_into().unwrap());

        Ok((
            Namespace {
                dataplane_id,
                instance_id,
            },
            &bytes[3..],
        ))
    }
}

#[derive(Debug, Clone)]
pub struct Prefix {
    namespace: Namespace,
}

impl Prefix {
    pub fn new(dataplane_id: u8, instance_id: u16) -> Self {
        Self {
            namespace: Namespace::new(dataplane_id, instance_id),
        }
    }

    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    pub fn cursor(&self) -> Vec<u8> {
        enc().append(&self.namespace).cursor_tag().build()
    }

    pub fn info(&self) -> Vec<u8> {
        enc().info_tag().break_().append(&self.namespace()).build()
    }

    pub fn data<T: Encode + Decode>(&self, kind: &Reducer, data: &T) -> Vec<u8> {
        enc()
            .append(&self.namespace)
            .data_tag()
            .append_with_break(kind)
            .append(data)
            .build()
    }

    pub fn collection_metadata<T: Encode>(&self, kind: &CollectionIngestor, data: &T) -> Vec<u8> {
        enc()
            .append(&self.namespace)
            .collection_metadata_tag()
            .append_with_break(kind)
            .append(data)
            .build()
    }

    pub fn miner_metadata<T: Encode>(&self, kind: &MinerIngestor, data: &T) -> Vec<u8> {
        enc()
            .append(&self.namespace)
            .miner_metadata_tag()
            .append_with_break(kind)
            .append(data)
            .build()
    }

    pub fn rollback(&self, data: rollback::MetadataKey) -> Vec<u8> {
        enc()
            .append(&self.namespace)
            .rollback_tag()
            .break_()
            .append(&data)
            .build()
    }

    pub fn lock(&self) -> Vec<u8> {
        enc().append(&self.namespace).lock_tag().build()
    }

    pub fn batch_complete(&self, batch_id: u32) -> Vec<u8> {
        enc()
            .append(&self.namespace)
            .batch_complete_tag()
            .break_()
            .append(&batch_id)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instance_key_range() {
        let namespace = Namespace::new(1, 42);
        let range = namespace.instance_key_range();

        assert_eq!(range.start, namespace.encode());
        assert_eq!(range.end, vec![1, 0, 43]);
    }

    #[test]
    fn test_dataplane_key_range() {
        let namespace = Namespace::new(1, 42);
        let range = namespace.dataplane_key_range();

        assert_eq!(range.start, vec![1]);
        assert_eq!(range.end, vec![2]);
    }
}
