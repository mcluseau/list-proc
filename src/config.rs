#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Config {
    pub env: String,
    pub cmd: String,
    pub args: Vec<String>,
}
