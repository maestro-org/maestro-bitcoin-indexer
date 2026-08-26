use bitcoin::Txid;
use serde::{Deserialize, Serialize};

use super::{
    resolver::{
        BRC20Resolver, InscriptionIndices, InscriptionNumbers, InscriptonsResolverValue,
        ResolverValue, RunesResolverValue, TxIndices,
    },
    DBInt, DBSerde, KVTable,
};

// mempool block index -> block txs and resolver
pub struct MempoolKV;

impl KVTable<DBInt, DBSerde<MempoolBlockValue>> for MempoolKV {
    const CF_NAME: &'static str = "MempoolKV";
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MempoolBlockValue {
    pub merkle_root: [u8; 32],
    pub txs: Vec<(Txid, Vec<u8>)>,
    pub resolver: ResolverValue,
    pub output_runes_resolver: RunesResolverValue,
    pub successful_etches: TxIndices,
    pub successful_mints: TxIndices,
    pub output_inscriptions_resolver: InscriptonsResolverValue,
    pub valid_reinscriptions: InscriptionIndices,
    pub brc20_resolver: BRC20Resolver,
    pub new_inscriptions: InscriptionNumbers,
}

// DBInt(0) -> chain tip used when processing mempool, timestamp of mempool snapshot used
pub struct MempoolTipKV;

impl KVTable<DBInt, DBSerde<MempoolInfoValue>> for MempoolTipKV {
    const CF_NAME: &'static str = "MempoolTipKV";
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MempoolInfoValue {
    pub chain_tip: (u64, [u8; 32]),
    pub mempool_view_ts: u64,
}
