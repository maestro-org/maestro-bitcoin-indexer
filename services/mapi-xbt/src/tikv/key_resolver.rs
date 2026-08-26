use strum::Display;

#[derive(Display, Debug, Clone, Copy)]
#[strum(serialize_all = "kebab-case")]
pub enum Network {
    Mainnet,
    Testnet,
}

// Enum for ReducerType with associated blockchain information
#[derive(Display, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[strum(serialize_all = "kebab-case")]
pub enum ReducerType {
    BlockInfo,
    ContentByInscriptionId,
    InscriptionActivityByScriptHash,
    InscriptionActivityByTx,
    InscriptionActivityByTxV2,
    InscriptionUtxosByScriptHash,
    RuneUtxosByScriptHash,
    Brc20BalancesByScriptHash,
    Brc20TermsByTicker,
    BalancesByBrc20,
    UtxosByRuneId,
    EtchingByRuneId,
    MintsByRuneId,
    RuneIdByRuneName,
    RuneTxsByScriptHash,
    SatBalanceByScriptHash,
    SatTxsByScriptHash,
    ScriptByScriptHash,
    ScriptHashByAddressPayloadHash,
    SpendingTxByTxo,
    TotalInscriptionsByScriptHash,
    TotalOutputsByScriptHash,
    TotalSatInInputsByScriptHash,
    TotalSatInOutputsByScriptHash,
    TotalTxsByScriptHash,
    TotalUtxosByScriptHash,
    TransferInscriptionsByScriptHash,
    TxInfo,
    TxsByBlock,
    TxsByInscription,
    TxsByRuneId,
    TxsByScriptHash,
    UtxosByScriptHash,
    RuneBalancesByScriptHash,
    SatsPerVbByBlock,
    HeightByBlockHash,
    HeightByTimestamp,
    BlockByTxHash,
    BalancesByRuneId,
    HistoricalSatBalanceByScriptHash,

    // Inscription collections metadata ingestor
    InscriptionCollectionsMetadata,

    // Miners metadata ingestor
    MinerMetadata,
}
