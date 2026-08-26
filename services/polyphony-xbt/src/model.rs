use std::{
    collections::HashMap,
    fmt::{self, Debug},
};

use bitcoin::{Block, OutPoint, ScriptHash, Transaction, TxOut, Txid, consensus::Decodable};
use ord::InscriptionId;
use ordinals::RuneId;
use serde::{Deserialize, Serialize};

use crate::{
    crosscut::Point,
    prelude::*,
    reducers::{IncrOrDecr, ReducerOutput},
    sources::compressor::compressor_api::OrdinalRange,
};

#[derive(Default, Debug, Clone)]
pub struct BlockContext {
    input_resolver: HashMap<OutPoint, ContextUtxo>,
    runes_resolver: HashMap<OutPoint, Vec<(RuneId, u128)>>,
    pub rune_mint_idxs: Vec<u32>,
    pub rune_etch_idxs: Vec<u32>,
    inscriptions_resolver: HashMap<OutPoint, Vec<(u32, InscriptionId)>>,
    pub valid_reinscriptions: Vec<(u32, u32)>,
    brc20_resolver: HashMap<InscriptionId, Vec<BRC20Message>>,
    // map for new inscriptions, inscription ID -> inscription number
    new_inscriptions: HashMap<InscriptionId, u64>,
}

#[derive(Debug, Clone)]
pub struct ContextUtxo {
    pub height: u64,
    pub txo: TxOut,
    pub ords: Vec<OrdinalRange>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum BRC20Message {
    Deploy(Vec<u8>),
    Mint(Vec<u8>, u128, ScriptHash),
    Transfer(Vec<u8>, u128, OutPoint, ScriptHash),
    TransferInit(Vec<u8>, u128, ScriptHash),
}

impl BlockContext {
    pub fn new() -> Self {
        Self {
            input_resolver: HashMap::new(),
            runes_resolver: HashMap::new(),
            rune_mint_idxs: Vec::new(),
            rune_etch_idxs: Vec::new(),
            inscriptions_resolver: HashMap::new(),
            valid_reinscriptions: Vec::new(),
            brc20_resolver: HashMap::new(),
            new_inscriptions: HashMap::new(),
        }
    }

    pub fn insert_txo(
        &mut self,
        key: &OutPoint,
        height: u64,
        raw: Vec<u8>,
        ords: Vec<OrdinalRange>,
    ) {
        let txo = TxOut::consensus_decode_from_finite_reader(&mut &raw[..]).unwrap(); // TODO

        let utxo = ContextUtxo { height, txo, ords };

        self.input_resolver.insert(key.clone(), utxo);
    }

    pub fn insert_runes(&mut self, key: &OutPoint, runes: Vec<(RuneId, u128)>) {
        self.runes_resolver.insert(key.clone(), runes);
    }

    pub fn insert_inscriptions(&mut self, key: &OutPoint, inscriptions: Vec<(u32, InscriptionId)>) {
        self.inscriptions_resolver.insert(key.clone(), inscriptions);
    }

    pub fn insert_brc20(&mut self, key: &InscriptionId, action: BRC20Message) {
        self.brc20_resolver.entry(*key).or_default().push(action);
    }

    pub fn find_utxo(&self, key: &OutPoint) -> Result<ContextUtxo, Error> {
        let utxo = self
            .input_resolver
            .get(key)
            .ok_or_else(|| Error::missing_utxo(key))?;

        Ok(utxo.clone())
    }

    pub fn utxo_runes(&self, key: &OutPoint) -> Option<Vec<(RuneId, u128)>> {
        self.runes_resolver.get(key).cloned()
    }

    pub fn utxo_inscriptions(&self, key: &OutPoint) -> Option<Vec<(u32, InscriptionId)>> {
        self.inscriptions_resolver.get(key).cloned()
    }

    pub fn inscription_brc20s(&self, key: &InscriptionId) -> Option<Vec<BRC20Message>> {
        self.brc20_resolver.get(key).cloned()
    }

    pub fn brc20_existence(&self) -> bool {
        self.brc20_resolver
            .values()
            .any(|messages| !messages.is_empty())
    }

    pub fn insert_new_inscription(&mut self, key: &InscriptionId, inscription_num: u64) {
        self.new_inscriptions.insert(key.clone(), inscription_num);
    }

    pub fn new_inscriptions(&self, key: &InscriptionId) -> Option<u64> {
        self.new_inscriptions.get(key).cloned()
    }

    pub fn get_all_new_inscriptions(&self) -> &HashMap<InscriptionId, u64> {
        &self.new_inscriptions
    }
}

#[derive(Debug, Clone)]
pub struct MempoolInfo {
    pub chain_tip: (u64, [u8; 32]),
    pub mempool_view_ts: u64,
}

type Mutable = bool;
pub type TransactionsWithIds = Vec<(Transaction, Txid)>;

#[derive(Debug, Clone)]
pub enum EnrichedBlockPayload {
    RollForward(Point, Block, TransactionsWithIds, BlockContext, Mutable),
    RollBack(Point, Mutable),
    /// chain tip, vec of new mempool blocks, fetch duration in ms
    MempoolRefresh(
        MempoolInfo,
        Vec<(Point, TransactionsWithIds, BlockContext)>,
        u128,
    ),
}

impl EnrichedBlockPayload {
    pub fn roll_forward(
        point: Point,
        block: Block,
        txs: TransactionsWithIds,
        ctx: BlockContext,
        mutable: bool,
    ) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollForward(point, block, txs, ctx, mutable),
        }
    }

    pub fn roll_back(point: Point, mutable: bool) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollBack(point, mutable),
        }
    }

    pub fn mempool_refresh(
        mempool_info: MempoolInfo,
        blocks: Vec<(Point, TransactionsWithIds, BlockContext)>,
        fetch_duration_ms: u128,
    ) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::MempoolRefresh(mempool_info, blocks, fetch_duration_ms),
        }
    }
}

type ChainMutable = bool;

#[derive(Debug, Clone)]
pub enum StorageActionPayload {
    RollForward(Point, Vec<ReducerOutput>, ChainMutable),
    RollBack(Point, bool),
    /// mempool info, blocks with outputs, fetch duration ms, reduce duration ms
    MempoolRefresh(MempoolInfo, Vec<(Point, Vec<ReducerOutput>)>, u128, u128),
}

impl fmt::Display for StorageActionPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageActionPayload::RollForward(point, _, _) => {
                write!(f, "RollForward({})", point)
            }
            StorageActionPayload::RollBack(point, _) => {
                write!(f, "RollBack({})", point)
            }
            StorageActionPayload::MempoolRefresh(mp, blocks, _, _) => {
                write!(
                    f,
                    "MempoolRefresh({:?}, [{:?}])",
                    mp,
                    blocks.iter().map(|b| b.0).collect::<Vec<_>>()
                )
            }
        }
    }
}

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;
pub type Delta = u128;
pub type AggregatePoint = u128;

pub type Height = u64;

#[derive(Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum StorageAction {
    /// Set `Key` to `Value`
    Set(Key, Value),

    /// Set `Key` to `Value`, but with the constraint that we MUST know that `Key` cannot already
    /// have a value. Use `Set` if you are not sure. This is an optimisation where we do not need to
    /// check the value of the key to know the inverse action, the inverse action is always delete
    /// instead of setting the key to the previous value because the key cannot have existed
    /// previously.
    SetOnce(Key, Value),

    /// Set `Key` to `Value`, but with the constraint that it is OK to not delete this Key if the
    /// block responsible for writing to it was rolledback. This is only rarely suitable, in cases
    /// where the value for a specific key will never change AND it is OK if the data persists even
    /// if the block rolled back. This is an optimisation where we don't need to fetch the previous
    /// value of the key and we don't need to store an inverse action in the rollback buffer.
    SetPermanent(Key, Value),

    /// Delete the KV pair with key `Key`
    Delete(Key),

    /// Set `Key` to `Value` only if `Key` does not already point to a value
    Insert(Key, Value),

    /// Set `Key` to `Value` only if `Key` does not already point to a value. Use only if it is OK
    /// to not delete this key if the block responsible for writing to it was rolledback. Typical
    /// usage for this variant includes storing data upon first seeing some on-chain event.
    InsertPermanent(Key, Value),

    /// Increment the value at `Key` by `Delta`
    Increment(Key, Delta),

    /// Decrement the value at `Key` by `Delta`
    /// (If value at Key exists, it must be big endian u64)
    Decrement(Key, Delta),

    /// Decrement the value at `Key` by `Delta` (do not remove the key if the resulting value is 0)
    DecrementNoDelete(Key, Delta),

    PointAggregate(Key, Vec<(AggregatePoint, IncrOrDecr<Delta>)>),
}

impl fmt::Debug for StorageAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Set(k, v) => write!(
                f,
                "StorageAction::Set([{}] -> [{}])",
                hex::encode(&k),
                hex::encode(&v)
            ),
            Self::SetOnce(k, v) => write!(
                f,
                "StorageAction::SetOnce([{}] -> [{}])",
                hex::encode(&k),
                hex::encode(&v)
            ),
            Self::SetPermanent(k, v) => write!(
                f,
                "StorageAction::SetPermanent([{}] -> [{}])",
                hex::encode(&k),
                hex::encode(&v)
            ),
            Self::Insert(k, v) => write!(
                f,
                "StorageAction::Insert([{}] -> [{}])",
                hex::encode(&k),
                hex::encode(&v)
            ),
            Self::InsertPermanent(k, v) => write!(
                f,
                "StorageAction::InsertPermanent([{}] -> [{}])",
                hex::encode(&k),
                hex::encode(&v)
            ),
            Self::Delete(k) => write!(f, "StorageAction::Del({})", hex::encode(&k)),
            Self::Increment(k, d) => {
                write!(f, "StorageAction Incr([{}] += {})", hex::encode(&k), d)
            }
            Self::Decrement(k, d) => {
                write!(f, "StorageAction Decr([{}] -= {})", hex::encode(&k), d)
            }
            Self::DecrementNoDelete(k, d) => {
                write!(f, "StorageAction DecrNoD([{}] -= {})", hex::encode(&k), d)
            }
            Self::PointAggregate(k, points) => {
                write!(
                    f,
                    "StorageAction::PointAggreagate([{}] += [{:?}])",
                    hex::encode(&k),
                    points
                )
            }
        }
    }
}

impl StorageAction {
    pub fn key(&self) -> &Vec<u8> {
        match self {
            StorageAction::Set(k, _) => k,
            StorageAction::SetOnce(k, _) => k,
            StorageAction::SetPermanent(k, _) => k,
            StorageAction::Delete(k) => k,
            StorageAction::Insert(k, _) => k,
            StorageAction::InsertPermanent(k, _) => k,
            StorageAction::Increment(k, _) => k,
            StorageAction::Decrement(k, _) => k,
            StorageAction::DecrementNoDelete(k, _) => k,
            StorageAction::PointAggregate(k, _) => k,
        }
    }

    pub fn into_key(self) -> Vec<u8> {
        match self {
            StorageAction::Set(k, _) => k,
            StorageAction::SetOnce(k, _) => k,
            StorageAction::SetPermanent(k, _) => k,
            StorageAction::Delete(k) => k,
            StorageAction::Insert(k, _) => k,
            StorageAction::InsertPermanent(k, _) => k,
            StorageAction::Increment(k, _) => k,
            StorageAction::Decrement(k, _) => k,
            StorageAction::DecrementNoDelete(k, _) => k,
            StorageAction::PointAggregate(k, _) => k,
        }
    }

    /// Returns true if the previous value of the key is required in order to
    /// perform the storage action.
    pub fn requires_previous_value(&self) -> bool {
        match self {
            StorageAction::Increment(_, _) => true,
            StorageAction::Decrement(_, _) => true,
            StorageAction::DecrementNoDelete(_, _) => true,
            StorageAction::Insert(_, _) => true,
            StorageAction::InsertPermanent(_, _) => true,
            StorageAction::Set(_, _) => false,
            StorageAction::SetPermanent(_, _) => false,
            StorageAction::SetOnce(_, _) => false,
            StorageAction::Delete(_) => false,
            StorageAction::PointAggregate(_, _) => true,
        }
    }

    /// Panics if StorageActions can't be merged
    pub fn merge(self, incoming: StorageAction) -> Option<StorageAction> {
        assert_eq!(
            self.key(),
            incoming.key(),
            "trying to merge actions with different keys"
        );
        match self {
            Self::Increment(k, pd) => {
                match incoming {
                    Self::Increment(nk, nd) => Some(Self::Increment(nk, pd + nd)),
                    Self::Decrement(nk, nd) => {
                        if pd == nd {
                            None
                        } else if pd > nd {
                            Some(Self::Increment(nk, pd - nd))
                        } else {
                            // pd < nd
                            Some(Self::Decrement(nk, nd - pd))
                        }
                    }
                    Self::DecrementNoDelete(nk, nd) => {
                        // unlike Decrement, if pd == nd, we will write 0 value if the
                        // key doesn't exist to align with the NoDelete behaviour
                        if pd >= nd {
                            Some(Self::Increment(nk, pd - nd))
                        } else {
                            // pd < nd
                            Some(Self::DecrementNoDelete(nk, nd - pd))
                        }
                    }
                    a => panic!(
                        "unexpected INCR/DECR type pair in storage action merge {a:?} on {k:?}"
                    ),
                }
            }
            Self::Decrement(k, pd) => match incoming {
                Self::Decrement(nk, nd) => Some(Self::Decrement(nk, pd + nd)),
                Self::Increment(nk, nd) => {
                    if pd == nd {
                        None
                    } else if pd > nd {
                        Some(Self::Decrement(nk, pd - nd))
                    } else {
                        Some(Self::Increment(nk, nd - pd))
                    }
                }
                a => panic!("trying to merge SET/DEL/INS or NoDel on DECR {a:?} {k:?} {pd:?}"),
            },
            Self::DecrementNoDelete(k, pd) => match incoming {
                Self::DecrementNoDelete(nk, nd) => Some(Self::DecrementNoDelete(nk, pd + nd)),
                Self::Increment(nk, nd) => {
                    // unlike Decrement, if pd == nd, we will write 0 value if the
                    // key doesn't exist to align with the NoDelete behaviour
                    if pd >= nd {
                        Some(Self::DecrementNoDelete(nk, pd - nd))
                    } else {
                        Some(Self::Increment(nk, nd - pd))
                    }
                }
                a => panic!("trying to merge SET/DEL/INS or Del on DECR {a:?} {k:?} {pd:?}"),
            },
            Self::Set(k, v) => match incoming {
                a @ (Self::Increment(_, _)
                | Self::Decrement(_, _)
                | Self::DecrementNoDelete(_, _)) => {
                    panic!("trying to merge INCR/DECR on SET {a:?} {k:?}")
                }
                a @ Self::SetOnce(_, _) => panic!("trying to merge SETONCE on SET {a:?} {k:?}"),
                a @ Self::SetPermanent(_, _) => {
                    panic!("trying to merge SETPERM on SET {a:?} {k:?}")
                }
                a @ Self::InsertPermanent(_, _) => {
                    panic!("trying to merge INSPERM on SET {a:?} {k:?}")
                }
                a @ Self::PointAggregate(_, _) => {
                    panic!("trying to merge POINTAGG on SET {a:?} {k:?}")
                }
                a @ (Self::Set(_, _) | Self::Delete(_)) => Some(a), // overwrite SET with SET/DEL
                Self::Insert(_, _) => Some(Self::Set(k, v)), // do nothing - don't overwrite with an INS
            },
            Self::SetOnce(k, _) => match incoming {
                a @ (Self::Increment(_, _)
                | Self::Decrement(_, _)
                | Self::DecrementNoDelete(_, _)) => {
                    panic!("trying to merge INCR/DECR on SETONCE {a:?} {k:?}")
                }
                a @ Self::Set(_, _) => panic!("trying to merge SET on SETONCE {a:?} {k:?}"),
                a @ Self::SetOnce(_, _) => panic!("trying to merge SETONCE on SETONCE {a:?} {k:?}"),
                a @ Self::SetPermanent(_, _) => {
                    panic!("trying to merge SETPERM on SETONCE {a:?} {k:?}")
                }
                Self::Delete(_) => None, // Delete counter-acts a SetOnce
                a @ Self::Insert(_, _) => panic!("trying to merge INS on SETONCE {a:?} {k:?}"),
                a @ Self::InsertPermanent(_, _) => {
                    panic!("trying to merge INSPERM on SETONCE {a:?} {k:?}")
                }
                a @ Self::PointAggregate(_, _) => {
                    panic!("trying to merge POINTAGG on SETONCE {a:?} {k:?}")
                }
            },
            Self::SetPermanent(k, v) => match incoming {
                a @ (Self::Increment(_, _)
                | Self::Decrement(_, _)
                | Self::DecrementNoDelete(_, _)) => {
                    panic!("trying to merge INCR/DECR on SETPERM {a:?} {k:?}")
                }
                a @ Self::Set(_, _) => panic!("trying to merge SET on SETPERM {a:?} {k:?}"),
                a @ Self::SetOnce(_, _) => panic!("trying to merge SETONCE on SETPERM {a:?} {k:?}"),
                a @ Self::Delete(_) => panic!("trying to merge DEL on SETPERM {a:?} {k:?}"),
                Self::SetPermanent(_, _) => Some(Self::SetPermanent(k, v)), // do nothing, it's supposed to be permanent
                a @ Self::Insert(_, _) => panic!("trying to merge INS on SETPERM {a:?} {k:?}"),
                a @ Self::InsertPermanent(_, _) => {
                    panic!("trying to merge INSPERM on SETPERM {a:?} {k:?}")
                }
                a @ Self::PointAggregate(_, _) => {
                    panic!("trying to merge POINTAGG on SETPERM {a:?} {k:?}")
                }
            },
            Self::Delete(k) => match incoming {
                a @ (Self::Increment(_, _)
                | Self::Decrement(_, _)
                | Self::DecrementNoDelete(_, _)) => {
                    panic!("trying to merge INCR/DECR on DEL {a:?} {k:?}")
                }
                a @ Self::SetOnce(_, _) => {
                    panic!("trying to merge SETONCE on DEL {a:?} {k:?}")
                }
                a @ Self::SetPermanent(_, _) => {
                    panic!("trying to merge SETPERM on DEL {a:?} {k:?}")
                }
                a @ Self::InsertPermanent(_, _) => {
                    panic!("trying to merge INSPERM on DEL {a:?} {k:?}")
                }
                a @ Self::PointAggregate(_, _) => {
                    panic!("trying to merge POINTAGG on DEL {a:?} {k:?}")
                }
                // Delete merged with Insert is a Set, because the value is cleared
                Self::Insert(k, v) => Some(Self::Set(k, v)),
                // Overwrite DEL with SET/DEL
                a @ (Self::Set(_, _) | Self::Delete(_)) => Some(a),
            },
            Self::Insert(k, v) => match incoming {
                a @ (Self::Increment(_, _)
                | Self::Decrement(_, _)
                | Self::DecrementNoDelete(_, _)) => {
                    panic!("trying to merge INCR/DECR on INS {a:?} {k:?}")
                }
                a @ Self::SetOnce(_, _) => {
                    panic!("trying to merge SETONCE on INS {a:?} {k:?}")
                }
                a @ Self::SetPermanent(_, _) => {
                    panic!("trying to merge SETPERM on INS {a:?} {k:?}")
                }
                a @ Self::InsertPermanent(_, _) => {
                    panic!("trying to merge INSPERM on INS {a:?} {k:?}")
                }
                a @ Self::PointAggregate(_, _) => {
                    panic!("trying to merge POINTAGG on INS {a:?} {k:?}")
                }
                // Overwrite INS with SET/DEL
                a @ (Self::Set(_, _) | Self::Delete(_)) => Some(a),
                // Don't overwrite an INS with another INS
                Self::Insert(_, _) => Some(Self::Insert(k, v)),
            },
            Self::InsertPermanent(k, v) => match incoming {
                // `InsertPermanent`s can only be merged with other `InsertPermanent`s, in which
                // case `self` is prioritized over the `StorageAction` passed as argument.
                a @ (Self::Increment(_, _)
                | Self::Decrement(_, _)
                | Self::DecrementNoDelete(_, _)) => {
                    panic!("trying to merge INCR/DECR on INSPERM {a:?} {k:?}")
                }
                a @ Self::SetOnce(_, _) => {
                    panic!("trying to merge SETONCE on INSPERM {a:?} {k:?}")
                }
                a @ Self::SetPermanent(_, _) => {
                    panic!("trying to merge SETPERM on INSPERM {a:?} {k:?}")
                }
                // Overwrite INS with SET/DEL
                a @ Self::Set(_, _) => {
                    panic!("trying to merge SET on INSPERM {a:?} {k:?}")
                }
                a @ Self::Delete(_) => {
                    panic!("trying to merge DEL on INSPERM {a:?} {k:?}")
                }
                a @ Self::Insert(_, _) => {
                    panic!("trying to merge INS on INSPERM {a:?} {k:?}")
                }
                a @ Self::PointAggregate(_, _) => {
                    panic!("trying to merge POINTAGG on INSPERM {a:?} {k:?}")
                }
                // Don't overwrite an INSPERM with another INSPERM
                Self::InsertPermanent(_, _) => Some(Self::InsertPermanent(k, v)),
            },
            Self::PointAggregate(prev_k, prev_points) => match incoming {
                Self::PointAggregate(new_k, new_points) => {
                    if new_k == prev_k {
                        let prev_points = prev_points.into_iter().collect::<HashMap<_, _>>();
                        let mut new_points = new_points.into_iter().collect::<HashMap<_, _>>();
                        let mut out_points = vec![];

                        // Combine point deltas.
                        for (prev_point, prev_delta) in prev_points {
                            if let Some(new_delta) = new_points.remove(&prev_point) {
                                out_points.push((prev_point, prev_delta + new_delta))
                            } else {
                                out_points.push((prev_point, prev_delta))
                            }
                        }

                        out_points.extend(new_points);
                        out_points.sort_by_key(|(x, _)| *x);

                        Some(Self::PointAggregate(prev_k, out_points))
                    } else {
                        panic!("trying to merge pair of PointAggregates with unmatching keys.")
                    }
                }
                _ => panic!("trying to merge PointAggregate with other"),
            },
        }
    }

    /// The diff between action A and action B is the action which we would need to take after
    /// action A has been applied to a key K in order for the effect to be the same as if we
    /// applied action B directly. That is, given key K, applying A and then diff(A, B) on it
    /// should have the same effect as applying B directly.
    /// For example:
    ///     diff(increment(100), increment(100)) -> None
    ///     diff(increment(100), increment(300)) -> increment(200)
    ///     diff(increment(300), increment(100)) -> decrement(200)
    ///     diff(increment(100), decrement(300)) -> decrement(400)
    ///     diff(set(AAA), set(AAA)) -> None
    ///     diff(set(AAA), set(BBB)) -> set(BBB)
    ///
    /// Its useful because if we need to undo action A, then apply action B, we can instead do a
    /// single action (the diff of action A and action B) and get the same result.
    pub fn diff(self, incoming: StorageAction) -> Option<StorageAction> {
        assert_eq!(
            self.key(),
            incoming.key(),
            "trying to diff actions with different keys"
        );

        match self {
            Self::Increment(k, pd) => match incoming {
                Self::Increment(nk, nd) => {
                    if nd == pd {
                        None
                    } else if nd > pd {
                        Some(Self::Increment(nk, nd.checked_sub(pd).unwrap()))
                    } else {
                        Some(Self::Decrement(nk, pd.checked_sub(nd).unwrap()))
                    }
                }
                Self::Decrement(nk, nd) => Some(Self::Decrement(nk, pd.checked_add(nd).unwrap())),
                Self::DecrementNoDelete(nk, nd) => {
                    Some(Self::DecrementNoDelete(nk, pd.checked_add(nd).unwrap()))
                }
                a @ Self::PointAggregate(_, _) => {
                    panic!("trying to merge POINTAGG on INCR {a:?} {k:?}")
                }
                a => panic!("unexpected INCR/DECR type pair in storage action diff {a:?} on {k:?}"),
            },
            Self::Decrement(k, pd) => match incoming {
                Self::Decrement(nk, nd) => {
                    if nd == pd {
                        None
                    } else if nd > pd {
                        Some(Self::Decrement(nk, nd.checked_sub(pd).unwrap()))
                    } else {
                        Some(Self::Increment(nk, pd.checked_sub(nd).unwrap()))
                    }
                }
                Self::Increment(nk, nd) => Some(Self::Increment(nk, pd.checked_add(nd).unwrap())),
                a @ Self::PointAggregate(_, _) => {
                    panic!("trying to merge POINTAGG on DECR {a:?} {k:?}")
                }
                a => panic!("trying to diff SET/DEL/INS on DECR {a:?} {k:?} {pd:?}"),
            },
            Self::DecrementNoDelete(k, pd) => match incoming {
                Self::DecrementNoDelete(nk, nd) => {
                    if nd == pd {
                        None
                    } else if nd > pd {
                        Some(Self::DecrementNoDelete(nk, nd.checked_sub(pd).unwrap()))
                    } else {
                        Some(Self::Increment(nk, pd.checked_sub(nd).unwrap()))
                    }
                }
                Self::Increment(nk, nd) => Some(Self::Increment(nk, pd.checked_add(nd).unwrap())),
                a @ Self::PointAggregate(_, _) => {
                    panic!("trying to merge POINTAGG on DECR {a:?} {k:?}")
                }
                a => panic!("trying to diff SET/DEL/INS on DECR {a:?} {k:?} {pd:?}"),
            },
            Self::Set(k, v) => match incoming {
                Self::Set(_, nv) if v == nv => None,
                a @ (Self::Set(_, _) | Self::Delete(_)) => Some(a),
                Self::Insert(_, _) | Self::InsertPermanent(_, _) => panic!("insert diff on SET"),
                a @ (Self::Increment(_, _)
                | Self::Decrement(_, _)
                | Self::DecrementNoDelete(_, _)) => {
                    panic!("trying to diff INCR/DECR on SET {a:?} {k:?}")
                }
                a @ Self::SetOnce(_, _) => panic!("trying to diff SETONCE on SET {a:?} {k:?}"),
                a @ Self::SetPermanent(_, _) => {
                    panic!("trying to diff SETPERM on SET {a:?} {k:?}")
                }
                a @ Self::PointAggregate(_, _) => {
                    panic!("trying to merge POINTAGG on SET {a:?} {k:?}")
                }
            },
            Self::SetOnce(k, v) => match incoming {
                Self::SetOnce(_, nv) if v == nv => None,
                // setonce check will show if we overwrite the existing value, so use set
                Self::SetOnce(_, nv) => Some(Self::Set(k, nv)),
                a @ Self::Delete(_) => Some(a),
                a @ (Self::Increment(_, _)
                | Self::Decrement(_, _)
                | Self::DecrementNoDelete(_, _)) => {
                    panic!("trying to diff INCR/DECR on SETONCE {a:?} {k:?}")
                }
                a @ Self::Set(_, _) => panic!("trying to merge SET on SETONCE {a:?} {k:?}"),
                a @ Self::SetPermanent(_, _) => {
                    panic!("trying to diff SETPERM on SETONCE {a:?} {k:?}")
                }
                a @ (Self::Insert(_, _) | Self::InsertPermanent(_, _)) => {
                    panic!("trying to diff INS on SETONCE {a:?} {k:?}")
                }
                a @ Self::PointAggregate(_, _) => {
                    panic!("trying to merge POINTAGG on SETONCE {a:?} {k:?}")
                }
            },
            Self::SetPermanent(k, v) => match incoming {
                Self::SetPermanent(_, nv) if v == nv => None,
                a @ (Self::Increment(_, _)
                | Self::Decrement(_, _)
                | Self::DecrementNoDelete(_, _)) => {
                    panic!("trying to diff INCR/DECR on SETPERM {a:?} {k:?}")
                }
                a @ Self::Set(_, _) => panic!("trying to diff SET on SETPERM {a:?} {k:?}"),
                a @ Self::SetOnce(_, _) => panic!("trying to diff SETONCE on SETPERM {a:?} {k:?}"),
                a @ Self::Delete(_) => panic!("trying to diff DEL on SETPERM {a:?} {k:?}"),
                a @ Self::SetPermanent(_, _) => {
                    panic!("trying to diff SETPERM on SETPERM {a:?} {k:?}")
                }
                a @ (Self::Insert(_, _) | Self::InsertPermanent(_, _)) => {
                    panic!("trying to diff INS on SETPERM {a:?} {k:?}")
                }
                a @ Self::PointAggregate(_, _) => {
                    panic!("trying to merge POINTAGG on SETPERM {a:?} {k:?}")
                }
            },
            Self::Delete(k) => match incoming {
                Self::Delete(_) => None,
                a @ (Self::Set(_, _) | Self::SetOnce(_, _) | Self::Insert(_, _)) => Some(a),
                a @ (Self::Increment(_, _)
                | Self::Decrement(_, _)
                | Self::DecrementNoDelete(_, _)) => {
                    panic!("trying to diff INCR/DECR on DEL {a:?} {k:?}")
                }
                a @ Self::SetPermanent(_, _) => {
                    panic!("trying to diff SETPERM on DEL {a:?} {k:?}")
                }
                a @ Self::InsertPermanent(_, _) => {
                    panic!("trying to diff INSPERM on DEL {a:?} {k:?}")
                }
                a @ Self::PointAggregate(_, _) => {
                    panic!("trying to merge POINTAGG on DEL {a:?} {k:?}")
                }
            },
            Self::Insert(k, v) => match incoming {
                Self::Insert(_, nv) if v == nv => None,
                a @ (Self::Set(_, _) | Self::Delete(_) | Self::Insert(_, _)) => Some(a),
                a @ (Self::Increment(_, _)
                | Self::Decrement(_, _)
                | Self::DecrementNoDelete(_, _)) => {
                    panic!("trying to merge INCR/DECR on INS {a:?} {k:?}")
                }
                a @ Self::SetOnce(_, _) => {
                    panic!("trying to merge SETONCE on INS {a:?} {k:?}")
                }
                a @ Self::SetPermanent(_, _) => {
                    panic!("trying to merge SETPERM on INS {a:?} {k:?}")
                }
                a @ Self::InsertPermanent(_, _) => {
                    panic!("trying to merge SETPERM on INS {a:?} {k:?}")
                }
                a @ Self::PointAggregate(_, _) => {
                    panic!("trying to merge POINTAGG on INS {a:?} {k:?}")
                }
            },
            Self::InsertPermanent(k, _) => match incoming {
                Self::InsertPermanent(_, _) => None,
                a @ (Self::Set(_, _) | Self::Delete(_) | Self::Insert(_, _)) => {
                    panic!("trying to merge SET/DEL/INS on INSPERM {a:?} {k:?}")
                }
                a @ (Self::Increment(_, _)
                | Self::Decrement(_, _)
                | Self::DecrementNoDelete(_, _)) => {
                    panic!("trying to merge INCR/DECR on INS {a:?} {k:?}")
                }
                a @ Self::SetOnce(_, _) => {
                    panic!("trying to merge SETONCE on INS {a:?} {k:?}")
                }
                a @ Self::SetPermanent(_, _) => {
                    panic!("trying to merge SETPERM on INS {a:?} {k:?}")
                }
                a @ Self::PointAggregate(_, _) => {
                    panic!("trying to merge POINTAGG on INSPERM {a:?} {k:?}")
                }
            },
            Self::PointAggregate(prev_k, prev_points) => match incoming {
                // Example: given key k, suppose
                //          A = PointAggregate(k, [(1, Increment(10)), (2, Decrement(2))])
                // and
                //          B = PointAggregate(k, [(1, Increment(1)), (3, Increment(6))])
                // Then, it must be that
                //          diff(A, B) = PointAggregate(k, [(1, Decrement(9)), (2, Increment(2)), (3, Increment(6))])
                // because applying A followed by diff(A, B) equals applying B directly.
                Self::PointAggregate(new_k, new_points) => {
                    if prev_k == new_k {
                        let prev_points = prev_points.into_iter().collect::<HashMap<_, _>>();
                        let mut new_points =
                            new_points.clone().into_iter().collect::<HashMap<_, _>>();
                        let mut out_points = vec![];

                        // Combine point deltas.
                        for (prev_point, prev_delta) in prev_points {
                            if let Some(new_delta) = new_points.remove(&prev_point) {
                                // Apply new_delta - prev_delta on prev_point, noting that if we apply
                                // prev_delta on prev_point followed by applying new_delta - prev_delta,
                                // we should get the same effect as applying new_delta directly.
                                out_points.push((prev_point, new_delta - prev_delta))
                            } else {
                                // Incoming has no udpates for prev_point, so revert prev_delta.
                                out_points.push((prev_point, -prev_delta))
                            }
                        }

                        out_points.extend(new_points);
                        out_points.sort_by_key(|(x, _)| *x);

                        Some(Self::PointAggregate(prev_k, out_points))
                    } else {
                        panic!("trying to diff pair of POINTAGG with different keys")
                    }
                }
                a @ Self::Set(_, _) => {
                    panic!("trying to diff SET on POINTAGG {a:?} {prev_k:?}")
                }
                a @ Self::Delete(_) => {
                    panic!("trying to diff DEL on POINTAGG {a:?} {prev_k:?}")
                }
                a @ Self::Insert(_, _) => {
                    panic!("trying to diff INS on POINTAGG {a:?} {prev_k:?}")
                }
                a @ Self::Increment(_, _) => {
                    panic!("trying to diff INCR on POINTAGG {a:?} {prev_k:?}")
                }
                a @ Self::Decrement(_, _) => {
                    panic!("trying to diff DECR on POINTAGG {a:?} {prev_k:?}")
                }
                a @ Self::DecrementNoDelete(_, _) => {
                    panic!("trying to diff DECRNODEL on POINTAGG {a:?} {prev_k:?}")
                }
                a @ Self::SetOnce(_, _) => {
                    panic!("trying to diff SETONCE on POINTAGG {a:?} {prev_k:?}")
                }
                a @ Self::SetPermanent(_, _) => {
                    panic!("trying to diff SETPERM on POINTAGG {a:?} {prev_k:?}")
                }
                a @ Self::InsertPermanent(_, _) => {
                    panic!("trying to diff INSPERM on POINTAGG {a:?} {prev_k:?}")
                }
            },
        }
    }

    /// Returns the total number of bytes for the key and value (estimate)
    pub fn size(&self) -> usize {
        match self {
            StorageAction::Set(k, v) => k.len() + v.len(),
            StorageAction::SetOnce(k, v) => k.len() + v.len(),
            StorageAction::SetPermanent(k, v) => k.len() + v.len(),
            StorageAction::Delete(k) => k.len(),
            StorageAction::Insert(k, v) => k.len() + v.len(),
            StorageAction::InsertPermanent(k, v) => k.len() + v.len(),
            StorageAction::Increment(k, _) => k.len() + 17,
            StorageAction::Decrement(k, _) => k.len() + 17,
            StorageAction::DecrementNoDelete(k, _) => k.len() + 17,
            StorageAction::PointAggregate(k, points) => {
                k.len() + (17 + points.len() * (16 + (1 + 16)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::StorageAction;

    #[test]
    fn test_merge() {
        let op1 = StorageAction::Increment(vec![1], 10);
        let op2 = StorageAction::Decrement(vec![1], 15);

        let mut map = HashMap::new();

        map.insert(vec![1], op1);
        map.insert(vec![2], StorageAction::Set(vec![2], vec![]));

        if let Some(prev) = map.remove(op2.key()) {
            println!("{:?}", prev.merge(op2));
        }
    }

    #[test]
    fn test_diff() {
        //     diff(increment(100), increment(100)) -> None
        //     diff(increment(100), increment(300)) -> increment(200)
        //     diff(increment(300), increment(100)) -> decrement(200)
        //     diff(increment(100), decrement(300)) -> decrement(400)
        //     diff(set(AAA), set(AAA)) -> None
        //     diff(set(AAA), set(BBB)) -> set(BBB)

        let op1 = StorageAction::Increment(vec![1], 100);
        let op2 = StorageAction::Increment(vec![1], 100);

        assert_eq!(op1.diff(op2), None);

        let op1 = StorageAction::Increment(vec![1], 100);
        let op2 = StorageAction::Increment(vec![1], 300);

        assert_eq!(op1.diff(op2), Some(StorageAction::Increment(vec![1], 200)));

        let op1 = StorageAction::Increment(vec![1], 300);
        let op2 = StorageAction::Increment(vec![1], 100);

        assert_eq!(op1.diff(op2), Some(StorageAction::Decrement(vec![1], 200)));

        let op1 = StorageAction::Increment(vec![1], 100);
        let op2 = StorageAction::Decrement(vec![1], 300);

        assert_eq!(op1.diff(op2), Some(StorageAction::Decrement(vec![1], 400)));

        let op1 = StorageAction::Set(vec![1], vec![0xAA]);
        let op2 = StorageAction::Set(vec![1], vec![0xAA]);

        assert_eq!(op1.diff(op2), None);

        let op1 = StorageAction::Set(vec![1], vec![0xAA]);
        let op2 = StorageAction::Set(vec![1], vec![0xBB]);

        assert_eq!(op1.diff(op2), Some(StorageAction::Set(vec![1], vec![0xBB])));
    }

    #[test]
    fn test_diff_point_aggregate() {
        use super::{
            IncrOrDecr::{Decrement, Increment},
            StorageAction::*,
        };

        // Example: given key k, suppose
        //          A = PointAggregate(k, [(1, Increment(10)), (2, Decrement(2))])
        // and
        //          B = PointAggregate(k, [(1, Increment(1)), (3, Increment(6))])
        // Then, it must be that
        //          diff(A, B) = PointAggregate(k, [(1, Decrement(9)), (2, Increment(2)), (3, Increment(6))])
        // because applying A followed by diff(A, B) equals applying B directly.
        let op1 = PointAggregate(vec![1], vec![(1, Increment(10)), (2, Decrement(2))]);
        let op2 = PointAggregate(vec![1], vec![(1, Increment(1)), (3, Increment(6))]);
        let res = PointAggregate(
            vec![1],
            vec![(1, Decrement(9)), (2, Increment(2)), (3, Increment(6))],
        );
        assert_eq!(op1.diff(op2), Some(res));

        // We prove that diffing against an empty list of update points equals negating the left
        // hand side of the diff.
        // Strictly speaking, this should never be the case because PointAggregate should always
        // involve a non-empty vector, but this test case helps us test negation in the context of
        // PointAggregate in a conceptual way.
        let op1_points = vec![(1, Increment(1)), (2, Increment(2)), (3, Increment(3))];
        let op1 = PointAggregate(vec![1], op1_points.clone());
        let op2 = PointAggregate(vec![1], vec![]);
        let res_points = op1_points
            .into_iter()
            .map(|(height, delta)| (height, -delta))
            .collect::<Vec<_>>();
        let res = PointAggregate(vec![1], res_points);
        assert_eq!(op1.diff(op2), Some(res));

        // No intersection between update points of storage actions op1 and op2. Conceptually
        // speaking, the result of diffing them consists therefore in inverting all operations of
        // op1 and inserting all operations of op2.
        let op1_points = vec![(2, Decrement(2)), (3, Increment(3)), (4, Decrement(4))];
        let op1 = PointAggregate(vec![2], op1_points.clone());
        let op2_points = vec![(1, Increment(1)), (5, Increment(5)), (6, Decrement(6))];
        let op2 = PointAggregate(vec![2], op2_points.clone());
        let inv_op1_points = op1_points
            .into_iter()
            .map(|(height, delta)| (height, -delta))
            .collect::<Vec<_>>();
        let mut res_points = vec![inv_op1_points, op2_points].concat();
        res_points.sort_by_key(|(x, _)| *x);
        let res = PointAggregate(vec![2], res_points);
        assert_eq!(op1.diff(op2), Some(res));
    }
}
