use super::Encode;

pub const PREFIX_CURSOR: u8 = b'C';
pub const PREFIX_DATA: u8 = b'D';
pub const PREFIX_INFO: u8 = b'I';
pub const PREFIX_ROLLBACK: u8 = b'R';
pub const PREFIX_COLLECTION_METADATA: u8 = b'T';
pub const PREFIX_MINER_METADATA: u8 = b'U';

pub const PREFIX_BATCHING_LOCK: u8 = b'L'; // 2pc key/lock for batched database tx commit
pub const PREFIX_BATCH_COMPLETE: u8 = b'B'; // keys noting which batches have been processed (when not mutable)

pub const BREAK: u8 = b'`';
pub const BREAK_1: u8 = BREAK + 1;

#[derive(Default, Clone)]
pub struct EncodeBuilder {
    output: Vec<u8>,
}

impl EncodeBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn break_(mut self) -> Self {
        self.output.push(BREAK);
        self
    }

    pub fn break1(mut self) -> Self {
        self.output.push(BREAK_1);
        self
    }

    pub fn cursor_tag(mut self) -> Self {
        self.output.push(PREFIX_CURSOR);
        self
    }

    pub fn lock_tag(mut self) -> Self {
        self.output.push(PREFIX_BATCHING_LOCK);
        self
    }

    pub fn batch_complete_tag(mut self) -> Self {
        self.output.push(PREFIX_BATCH_COMPLETE);
        self
    }

    pub fn info_tag(mut self) -> Self {
        self.output.push(PREFIX_INFO);
        self
    }

    pub fn data_tag(mut self) -> Self {
        self.output.push(PREFIX_DATA);
        self
    }

    pub fn rollback_tag(mut self) -> Self {
        self.output.push(PREFIX_ROLLBACK);
        self
    }

    pub fn collection_metadata_tag(mut self) -> Self {
        self.output.push(PREFIX_COLLECTION_METADATA);
        self
    }

    pub fn miner_metadata_tag(mut self) -> Self {
        self.output.push(PREFIX_MINER_METADATA);
        self
    }

    pub fn append_with_break<T: Encode>(self, data: &T) -> Self {
        self.append(&data).break_()
    }

    pub fn append<T: Encode>(mut self, data: &T) -> Self {
        self.output.extend(data.encode());
        self
    }

    pub fn build(self) -> Vec<u8> {
        self.output
    }
}
