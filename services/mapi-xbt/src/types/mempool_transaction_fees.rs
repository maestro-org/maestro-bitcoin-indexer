use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr, PickFirst};
use utoipa::ToSchema;

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[schema(
    example = "{\"ancestor\":\"0.00006405\",\"base\":\"0.00000305\",\"descendant\":\"0.00001628\",\"modified\":\"0.00000305\"}"
)]
pub struct TransactionFees {
    /// Ancestor fee
    #[serde_as(as = "PickFirst<(DisplayFromStr, _)>")]
    #[schema(example = "\"0.00000111\"", value_type = String)]
    pub ancestor: f64,

    /// Base fee
    #[serde_as(as = "PickFirst<(DisplayFromStr, _)>")]
    #[schema(example = "\"0.00000111\"", value_type = String)]
    pub base: f64,

    /// Descendant fee
    #[serde_as(as = "PickFirst<(DisplayFromStr, _)>")]
    #[schema(example = "\"0.0000119\"", value_type = String)]
    pub descendant: f64,

    /// Modified fee
    #[serde_as(as = "PickFirst<(DisplayFromStr, _)>")]
    #[schema(example = "\"0.00000111\"", value_type = String)]
    pub modified: f64,
}
