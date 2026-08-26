use std::collections::HashMap;

use crate::model::{Key, StorageAction};

use super::StorageActions;

#[derive(Clone)]
pub struct ActionMerger {
    pub actions: HashMap<Key, StorageAction>,
}

impl ActionMerger {
    pub fn new() -> Self {
        ActionMerger {
            actions: HashMap::new(),
        }
    }

    pub fn push_actions(&mut self, actions: StorageActions) {
        for action in actions {
            if let Some(prev) = self.actions.remove(action.key()) {
                if let Some(new_action) = prev.merge(action.clone()) {
                    self.actions.insert(action.into_key(), new_action);
                }
            } else {
                self.actions.insert(action.key().clone(), action);
            }
        }
    }

    pub fn into_merged_actions(self) -> StorageActions {
        self.actions.into_values().collect()
    }

    pub fn into_batch_actions_with_keys(self) -> HashMap<Key, StorageAction> {
        self.actions
    }
}
