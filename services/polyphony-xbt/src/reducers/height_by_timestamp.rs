/*
   Block Height by Block Timestamp

   Creates reducer outputs to store the block height associated to a block timestamp.
*/

use super::ReducerOutput;
use bitcoin::Block;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer;

#[derive(Clone, Debug)]
pub struct Output {
    // Block timestamp.
    pub timestamp: u32,

    // Block height.
    pub height: u64,
}

impl Reducer {
    pub fn reduce_block(
        &mut self,
        height: u64,
        block: &Option<Block>,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        match block {
            Some(block) => {
                outputs.push(ReducerOutput::HeightByTimestamp(Output {
                    timestamp: block.header.time,
                    height,
                }));
                Ok(())
            }
            None => return Ok(()),
        }
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer;

        super::Reducer::HeightByTimestamp(reducer)
    }
}
