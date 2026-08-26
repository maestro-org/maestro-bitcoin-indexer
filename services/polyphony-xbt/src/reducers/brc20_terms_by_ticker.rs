/*
   BRC20 Terms by Ticker

   Creates reducer outputs on deployment of BRC20 tokens specifying the
   deployment parameters.
*/

use bitcoin::{Transaction, Txid, hashes::Hash};
use ord::{Inscription, InscriptionId, ParsedEnvelope};
use serde::Deserialize;
use tracing::warn;

use crate::model::{self, BRC20Message};

use super::ReducerOutput;

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer;

#[derive(Clone, Debug)]
pub struct Output {
    pub ticker: Vec<u8>,
    pub max: u128,
    pub limit: u128,
    pub dec: u8,
    pub self_mint: bool,
    pub deploy_id: ([u8; 32], u32),
}

impl Reducer {
    pub fn reduce_block<'b>(
        &mut self,
        _height: u64,
        txs: &Vec<(Transaction, Txid)>,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        for (tx, txid) in txs {
            let envelopes = ParsedEnvelope::from_transaction(tx);

            for (idx, inscription) in envelopes.into_iter().enumerate() {
                let inscription_id = InscriptionId {
                    txid: *txid,
                    index: idx as u32,
                };

                if let Some(brc20_actions) = ctx.inscription_brc20s(&inscription_id) {
                    for action in brc20_actions {
                        match action {
                            BRC20Message::Deploy(_) => {
                                let x = unparsed_message(&inscription.payload).unwrap();
                                let deploy = DeployAction::parse(&x).unwrap();

                                outputs.push(ReducerOutput::Brc20TermsByTicker(Output {
                                    ticker: deploy.ticker,
                                    max: deploy.max,
                                    limit: deploy.limit,
                                    dec: deploy.dec,
                                    self_mint: deploy.self_mint,
                                    deploy_id: (*txid.as_byte_array(), idx as u32),
                                }))
                            }
                            _ => (),
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer;

        super::Reducer::Brc20TermsByTicker(reducer)
    }
}

pub fn unparsed_message(inscription: &Inscription) -> Option<RawProtocolMessage> {
    if let Some(ct) = inscription.content_type() {
        let ct = ct.split(";").next();

        match ct {
            Some("text/plain") | Some("application/json") => (),
            _ => return None,
        };
    } else {
        return None;
    }

    let body = if let Ok(b) = std::str::from_utf8(inscription.body().unwrap_or_default()) {
        b
    } else {
        return None;
    };

    let raw: RawProtocolMessage = if let Ok(x) = serde_json::from_str(body) {
        x
    } else {
        return None;
    };

    if raw.p.as_str() != "brc-20" {
        return None;
    }

    Some(raw)
}

#[derive(Deserialize)]
pub struct RawProtocolMessage {
    pub p: String,
    pub op: String,
    pub tick: String,
    pub max: Option<String>, // required for deploy
    pub lim: Option<String>,
    pub dec: Option<String>,
    pub self_mint: Option<String>,
    pub amt: Option<String>, // required for mint or transfer
}

pub static NUMBERS: &str = "0123456789";
pub static NUMBERS_AND_DOT: &str = "0123456789.";

#[derive(Debug, Clone)]
pub struct DeployAction {
    pub ticker: Vec<u8>,
    pub max: u128,
    pub limit: u128,
    pub dec: u8,
    pub self_mint: bool,
}

impl DeployAction {
    pub fn parse(raw: &RawProtocolMessage) -> Option<Self> {
        match raw.op.as_str() {
            "deploy" => {
                let self_mint = if let Some(x) = &raw.self_mint {
                    x.as_str() == "true"
                } else {
                    false
                };

                match raw.tick.as_bytes().len() {
                    4 if !self_mint => (),
                    5 if self_mint => (),
                    _ => {
                        warn!("invalid ticker len: {} {}", raw.tick, self_mint);
                        return None;
                    }
                };

                let dec = {
                    let dec_string = if let Some(x) = &raw.dec {
                        x.clone()
                    } else {
                        "18".into()
                    };

                    // all digits

                    if dec_string.chars().any(|c| !NUMBERS.contains(c)) {
                        warn!("dec non digit: {dec_string}");
                        return None;
                    }

                    // at least one digit

                    let dec: u8 = if let Ok(d) = dec_string.parse() {
                        d
                    } else {
                        warn!("dec parse: {dec_string}");
                        return None;
                    };

                    // max 18

                    if dec > 18 {
                        warn!("dec too large: {dec}");
                        return None;
                    }

                    dec
                };

                let max = if let Some(m) = raw.max.clone() {
                    let mut x = parse_decimal(m, dec)?;

                    // max can only be 0 if self mint is true, in which case max is
                    if x == 0 {
                        if self_mint {
                            x = (u64::MAX as u128)
                                .checked_mul(10u128.pow(dec as u32))
                                .unwrap() // TODO
                        } else {
                            warn!("max = 0 but not self mint");
                            return None;
                        }
                    }

                    x
                } else {
                    warn!("deploy with no max");
                    return None;
                };

                let limit = if let Some(l) = raw.lim.clone() {
                    let x = parse_decimal(l, dec)?;

                    if x == 0 {
                        warn!("limit 0");
                        return None;
                    }

                    x
                } else {
                    max // lim defaults to max
                };

                Some(DeployAction {
                    ticker: raw.tick.clone().to_lowercase().into_bytes(),
                    max,
                    limit,
                    dec,
                    self_mint,
                })
            }
            _ => None,
        }
    }
}

pub fn parse_decimal(m: String, dec: u8) -> Option<u128> {
    if m.chars().any(|c| !NUMBERS_AND_DOT.contains(c)) {
        warn!("non digit or dot: {m}");
        return None;
    }

    if m.starts_with(".") || m.ends_with(".") {
        warn!("max start or end dot: {m}");
        return None;
    }

    let parts: Vec<_> = m.split(".").collect();

    if parts.len() > 2 {
        warn!("max too many dots: {m}");
        return None;
    }

    let has_decimal = parts.len() == 2;

    let integer: u64 = {
        let integer_part = parts[0].to_string();

        // must not be greater than u64::MAX
        if let Ok(x) = integer_part.parse() {
            x
        } else {
            warn!("max int too large: {m}");
            return None;
        }
    };

    let out: u128 = if has_decimal {
        let mut decimal_part = parts[1].to_string();

        if decimal_part.len() > dec as usize {
            warn!("max too many dec: {m}");
            return None;
        } else {
            for _ in 0..(dec as usize - decimal_part.len()) {
                decimal_part.push('0');
            }
        }

        assert_eq!(decimal_part.len(), dec as usize);

        format!("{}{}", integer, decimal_part).parse().unwrap() // TODO
    } else {
        (integer as u128)
            .checked_mul(10u128.pow(dec as u32))
            .unwrap() // TODO
    };

    Some(out)
}
