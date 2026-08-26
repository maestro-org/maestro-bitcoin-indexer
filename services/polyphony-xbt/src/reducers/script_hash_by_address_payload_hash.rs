/*
   Script Hash by Address Payload Hash

   Creates reducer output for every output which maps an address (specifically
   hash of the address payload) to a hash of the script pubkey which the address
   is built from. The usage is that users can provide us with an address, and we
   can fetch the script hash of the script which the address is built from.

   For example, user provides an address to the UTxOs by address endpoint:
   -- parse address
   -- pull out the address payload and hash it
   -- fetch script hash by address payload hash
   -- fetch utxos by script hash

   UTxOs are guarded by scripts, not addresses, so this more correct approach,
   and by storing hashes we save storage.
*/

use bitcoin::{Address, Transaction, TxOut, Txid, hashes::Hash};
use serde::Deserialize;

use crate::model;

use super::ReducerOutput;

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer;

#[derive(Clone, Debug)]
pub struct Output {
    pub payload_hash: [u8; 20],
    pub script_hash: [u8; 20],
}

impl Reducer {
    fn process_produced_txo(
        &mut self,
        tx_output: &TxOut,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        let script_hash = tx_output.script_pubkey.script_hash();

        // safe to hardcode network as we only care about the payload
        if let Ok(addr) = Address::from_script(&tx_output.script_pubkey, bitcoin::Network::Bitcoin)
        {
            let payload_hash = addr.script_pubkey().script_hash();

            outputs.push(ReducerOutput::ScriptHashByAddressPayloadHash(Output {
                payload_hash: payload_hash.to_byte_array(),
                script_hash: script_hash.to_byte_array(),
            }));
        }

        Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        _height: u64,
        txs: &Vec<(Transaction, Txid)>,
        _ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        for (tx, _) in txs {
            for txo in tx.output.iter() {
                self.process_produced_txo(txo, outputs)?;
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer;

        super::Reducer::ScriptHashByAddressPayloadHash(reducer)
    }
}
