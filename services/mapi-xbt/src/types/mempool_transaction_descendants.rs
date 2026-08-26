use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[schema(example = json!(["53096370ea3aa168ffab984045ad580e9b8de93bd6d46f2a36a09cf615368919","b1cf09d014bf6fe73640e3c97325a2272ad9477adc9e0e8eb946073b9371793f"]))]
pub struct MempoolTransactionDescendants(Vec<String>);
