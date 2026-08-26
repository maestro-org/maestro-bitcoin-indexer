use ord::InscriptionId;

use crate::storage::kvtable::*;

use super::{
    inscriptions::{updater::BRC20Message, Counters},
    BlockHash, RuneId, TxoBody, TxoRef,
};

// block slot -> (hash, required transaction output bytes)
pub struct ResolverByHeightKV;

pub type ResolverValue = Vec<(TxoRef, TxoBody)>;
pub type RunesResolverValue = Vec<(TxoRef, Vec<(RuneId, u128)>)>; // outputs to runes (not inputs)
pub type InscriptonsResolverValue = Vec<(TxoRef, Vec<(u64, InscriptionId)>)>; // outputs to inscriptions and their offsets (not inputs)
                                                                              // pub type LostAndUnboundInscriptions
pub type InscriptionCounters = Counters;
pub type TxIndices = Vec<u32>;
pub type InscriptionIndices = Vec<(u32, u32)>;
pub type BRC20Resolver = Vec<(InscriptionId, BRC20Message)>;
pub type InscriptionNumbers = Vec<(InscriptionId, u64)>;

// (_, _, _, indices of txs with successful rune etchs, indices of txs with successful rune mints)
impl
    KVTable<
        DBInt,
        DBSerde<(
            BlockHash,
            ResolverValue,
            RunesResolverValue,
            TxIndices, // indices of txs with successful rune etchs
            TxIndices, // indices of txs with successful rune mints
            InscriptonsResolverValue,
            InscriptionIndices, // indices of inscriptions with valid reinscriptions
            InscriptionCounters,
            BRC20Resolver,
            InscriptionNumbers,
        )>,
    > for ResolverByHeightKV
{
    const CF_NAME: &'static str = "ResolverByHeightKV";
}
