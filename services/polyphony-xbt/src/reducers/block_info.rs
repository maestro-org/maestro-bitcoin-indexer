/*
   Block Info

   Creates reducer outputs with information about a block, including a flag to indicate if any of its transactions involved metaprotocols.
*/

use super::ReducerOutput;
use crate::{crosscut, model, prelude::*};
use bitcoin::{Block, OutPoint, Transaction, Txid, VarInt, block::Header, hashes::Hash};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer {
    policy: crosscut::policies::RuntimePolicy,
}

#[derive(Clone, Debug)]
pub struct Output {
    // Block height
    pub height: u64,

    // Block hash.
    pub block_hash: Option<[u8; 32]>,

    // Block size
    pub block_size: u64,

    // Number of weight units (WU) of the block.
    pub block_weight_units: u64,

    // The timestamp of the block, as claimed by the miner.
    pub timestamp: Option<u32>,

    // Total fees paid by all transactions in the block.
    pub total_fees: u128,

    // Total number of satoshis that went through this block, minus fees.
    pub total_volume: u128,

    // Total number of transactions.
    pub total_txs: u32,

    // Whether any of the inputs or outputs of any of the transactions in the block contains inscriptions.
    pub involves_inscriptions: bool,

    // Whether any of the inputs or outputs of any of the transactions in the block contains runes.
    pub involves_runes: bool,

    // Whether any of the inputs or outputs of any of the transactions in the block contains BRC-20 messages.
    pub involves_brc20: bool,

    // The `script_sig` byte array from the coinbase tx.
    pub coinbase_script_sig: Vec<u8>,
}

impl Reducer {
    pub fn reduce_block(
        &mut self,
        height: u64,
        txs: &Vec<(Transaction, Txid)>,
        block: &Option<Block>,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        let block_hash = block.as_ref().map(|b| b.block_hash().to_byte_array());
        let (block_size, block_weight_units) = block
            .as_ref()
            .map(|b| (b.total_size() as u64, b.weight().into()))
            .unwrap_or({
                // mempool block size and weight units must be computed manually
                // See https://github.com/rust-bitcoin/rust-bitcoin/blob/3b2363b2c66c4a3884af75476d85df1ec1ab76e8/bitcoin/src/blockdata/block.rs#L263
                // for how to compute total size and weight units
                let mut total_size = Header::SIZE;
                total_size += VarInt::from(txs.len()).size();
                total_size += txs.iter().map(|(tx, _)| tx.total_size()).sum::<usize>();

                let mut base_size = Header::SIZE;
                base_size += VarInt::from(txs.len()).size();
                base_size += txs.iter().map(|(tx, _)| tx.base_size()).sum::<usize>();

                (total_size as u64, (3 * base_size + total_size) as u64)
            });

        let timestamp = block.as_ref().map(|b| b.header.time);

        let mut total_fees = 0u128;

        let mut total_volume = 0u128;

        let total_txs = txs.len() as u32;

        let mut involves_inscriptions = false;
        let mut involves_runes = false;
        let mut involves_brc20 = false;

        let mut coinbase_script_sig = Vec::new();

        for (tx, tx_hash) in txs.iter() {
            let mut input_volume = 0;
            let mut output_volume = 0;

            // coinbase transactions have no inputs
            if !tx.is_coinbase() {
                for input in tx.input.iter() {
                    let outpoint = input.previous_output;

                    let resolved_utxo = ctx
                        .find_utxo(&outpoint)
                        .apply_policy(&self.policy)
                        .or_panic()?;

                    let resolved_utxo = match resolved_utxo {
                        Some(u) => u,
                        None => return Ok(()),
                    };

                    input_volume += resolved_utxo.txo.value.to_sat() as u128;

                    involves_inscriptions |= ctx.utxo_inscriptions(&outpoint).is_some();
                    involves_runes |= ctx.utxo_runes(&outpoint).is_some();
                    involves_brc20 |= ctx.brc20_existence();
                }
            } else {
                // coinbase tag is used to identify miner tag
                coinbase_script_sig = tx.input[0].script_sig.clone().into_bytes();
            }

            for (output_index, output) in tx.output.iter().enumerate() {
                let outpoint = OutPoint::new(*tx_hash, output_index as u32);

                output_volume += output.value.to_sat() as u128;

                involves_inscriptions |= ctx.utxo_inscriptions(&outpoint).is_some();
                involves_runes |= ctx.utxo_runes(&outpoint).is_some();
                involves_brc20 |= ctx.brc20_existence();
            }

            total_fees += input_volume.saturating_sub(output_volume);
            total_volume += output_volume;
        }

        outputs.push(ReducerOutput::BlockInfo(Output {
            height,
            block_hash,
            block_size,
            block_weight_units,
            timestamp,
            total_fees,
            total_volume,
            total_txs,
            involves_inscriptions,
            involves_runes,
            involves_brc20,
            coinbase_script_sig,
        }));
        Ok(())
    }
}

impl Config {
    pub fn plugin(self, policy: &crosscut::policies::RuntimePolicy) -> super::Reducer {
        let reducer = Reducer {
            policy: policy.clone(),
        };

        super::Reducer::BlockInfo(reducer)
    }
}
