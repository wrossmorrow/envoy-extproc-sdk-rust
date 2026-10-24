use async_trait::async_trait;
use std::any::Any;
use std::collections::HashMap;

use extproc::{
    AllHeaders, Error, ProcessingOptions, RequestContext, RequestPhase, RequestProcessor,
};
use serde_json::{Map, Value};

pub struct MaskerRequestProcessor {
    pub opts: ProcessingOptions,
    /// Paths (dot-delimited) to mask, per phase. Empty means "do not mask".
    pub masked: HashMap<String, Vec<String>>,
}

impl MaskerRequestProcessor {
    pub fn new(opts: ProcessingOptions) -> Self {
        Self {
            opts,
            masked: HashMap::from([
                (
                    "request".to_string(),
                    vec!["maskme".to_string(), "mask.me".to_string()],
                ),
                ("response".to_string(), Vec::new()),
            ]),
        }
    }

    fn mask_body(&self, json_paths: &[String], body: &[u8]) -> Result<Vec<u8>, Error> {
        let mut value: Value = serde_json::from_slice(body)?;
        if let Value::Object(map) = &mut value {
            for path in json_paths {
                let parts: Vec<&str> = path.split('.').collect();
                let _ = set_at_path(map, &parts, &Value::String("****".into()));
            }
        }
        Ok(serde_json::to_vec(&value).map_err(|e| format!("json marshal failed: {}", e))?)
    }
}

fn is_maybe_json(headers: &AllHeaders) -> bool {
    headers
        .values("content-type")
        .iter()
        .any(|t| t.trim().to_lowercase().starts_with("application/json"))
}

fn set_at_path(map: &mut Map<String, Value>, parts: &[&str], value: &Value) -> bool {
    if parts.len() == 1 {
        if map.contains_key(parts[0]) {
            map.insert(parts[0].to_string(), value.clone());
            return true;
        }
        return false;
    }
    let key = parts[0];
    let Some(child) = map.get_mut(key) else {
        return false;
    };
    let Some(child_map) = child.as_object_mut() else {
        return false;
    };
    set_at_path(child_map, &parts[1..], value)
}

#[async_trait]
impl RequestProcessor for MaskerRequestProcessor {
    fn name(&self) -> String {
        "masker".to_string()
    }

    fn options(&self) -> &ProcessingOptions {
        &self.opts
    }

    async fn process_request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.continue_request()
    }

    async fn process_request_trailers(
        &self,
        ctx: &mut RequestContext,
        _trailers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.continue_request()
    }

    async fn process_request_body(
        &self,
        ctx: &mut RequestContext,
        body: &[u8],
    ) -> Result<(), Error> {
        if !self.masked["request"].is_empty() && is_maybe_json(&ctx.all_headers) {
            match self.mask_body(&self.masked["request"], body) {
                Ok(masked) => ctx.replace_body(&masked)?,
                Err(e) => tracing::error!("masker error: {}", e),
            }
        }
        ctx.continue_request()
    }

    async fn process_response_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.set_value(
            "responseHeaders",
            Box::new(ctx.all_headers.clone()) as Box<dyn Any + Send + Sync>,
        )?;
        ctx.continue_request()
    }

    async fn process_response_body(
        &self,
        ctx: &mut RequestContext,
        body: &[u8],
    ) -> Result<(), Error> {
        if !self.masked["response"].is_empty() {
            if let Some(headers) = ctx
                .get_value("responseHeaders")
                .ok()
                .and_then(|v| v.downcast_ref::<AllHeaders>().cloned())
            {
                if is_maybe_json(&headers) {
                    if let Ok(masked) = self.mask_body(&self.masked["response"], body) {
                        ctx.replace_body(&masked)?;
                    }
                }
            }
        }
        ctx.continue_request()
    }

    async fn process_response_trailers(
        &self,
        ctx: &mut RequestContext,
        _trailers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.continue_request()
    }

    fn error_handler(&self, _ctx: &mut RequestContext, phase: RequestPhase, err: Error) {
        tracing::error!("Error in phase {:?}: {:?}", phase, err);
    }

    fn close(&self, _grace_period_seconds: i32) -> Result<(), Error> {
        Ok(())
    }
}
