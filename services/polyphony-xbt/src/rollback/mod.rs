use crate::{crosscut::Point, model::StorageAction};

pub mod buffer;

#[derive(Debug, Clone)]
pub struct PersistentBufferValue {
    pub point: Point,
    pub inverse_actions: Vec<StorageAction>,
}
