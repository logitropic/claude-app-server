use claude_app_server_protocol::AVAILABLE_MODELS;
use claude_app_server_protocol::ModelListResponse;

pub fn model_list_response() -> ModelListResponse {
    ModelListResponse {
        models: AVAILABLE_MODELS.to_vec(),
    }
}
