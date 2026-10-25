use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};

use envoy_extproc_sdk::{
    AllHeaders, Error, HeaderValue, ProcessingOptions, RequestContext, RequestPhase,
    RequestProcessor,
};
use sha2::{Digest, Sha256};

static DEDUP_CACHE: LazyLock<Mutex<HashMap<String, bool>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub struct DedupRequestProcessor {
    pub opts: ProcessingOptions,
}

fn lock_hasher(ctx: &RequestContext) -> Result<MutexGuard<'_, Sha256>, Error> {
    let hasher = ctx.get_value::<Arc<Mutex<Sha256>>>()?;
    Ok(hasher
        .lock()
        .map_err(|_| "hasher lock failed".to_string())?)
}

fn compute_digest(hasher: &mut Sha256) -> String {
    hex::encode(hasher.clone().finalize())
}

fn set_digest(ctx: &mut RequestContext, digest: &str) -> Result<(), Error> {
    ctx.set_value(digest.to_string())?;
    ctx.add_header(
        "x-extproc-request-digest",
        &HeaderValue {
            value: String::new(),
            raw_value: Some(digest.as_bytes().to_vec()),
        },
    )?;
    Ok(())
}

fn report_digest(ctx: &mut RequestContext) -> Result<(), Error> {
    if ctx.end_of_stream {
        if let Ok(digest) = ctx.get_value::<String>() {
            ctx.add_header(
                "x-extproc-request-digest",
                &HeaderValue {
                    value: String::new(),
                    raw_value: Some(digest.as_bytes().to_vec()),
                },
            )?;
        }
    }
    Ok(())
}

fn is_request_cached(digest: &str) -> bool {
    DEDUP_CACHE.lock().unwrap().contains_key(digest)
}

fn cache_request(digest: &str) {
    DEDUP_CACHE.lock().unwrap().insert(digest.to_string(), true);
}

fn uncache_request(digest: &str) {
    DEDUP_CACHE.lock().unwrap().remove(digest);
}

fn dedupable(ctx: &RequestContext) -> bool {
    matches!(ctx.method.as_str(), "PUT" | "POST" | "PATCH")
}

#[async_trait]
impl RequestProcessor for DedupRequestProcessor {
    fn name(&self) -> String {
        "dedup".to_string()
    }

    fn options(&self) -> &ProcessingOptions {
        &self.opts
    }

    async fn process_request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.set_value(Arc::new(Mutex::new(Sha256::new())))?;
        let digest = {
            let mut hasher = lock_hasher(ctx)?;
            hasher.update(format!("{}:{}", ctx.method, ctx.path).as_bytes());
            if ctx.end_of_stream {
                Some(compute_digest(&mut hasher))
            } else {
                None
            }
        };
        if let Some(digest) = digest {
            set_digest(ctx, &digest)?;
            if dedupable(ctx) && is_request_cached(&digest) {
                let headers = HashMap::new();
                return ctx.cancel_request(409, &headers, b"");
            }
            cache_request(&digest);
        }
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
        let digest = {
            let mut hasher = lock_hasher(ctx)?;
            hasher.update(b":");
            hasher.update(body);
            if ctx.end_of_stream {
                Some(compute_digest(&mut hasher))
            } else {
                None
            }
        };
        if let Some(digest) = digest {
            set_digest(ctx, &digest)?;
            if dedupable(ctx) && is_request_cached(&digest) {
                let headers = HashMap::new();
                return ctx.cancel_request(409, &headers, b"");
            }
            cache_request(&digest);
        }
        ctx.continue_request()
    }

    async fn process_response_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        report_digest(ctx)?;
        let digest = ctx.get_value::<String>().ok().cloned().unwrap_or_default();
        uncache_request(&digest);
        ctx.continue_request()
    }

    async fn process_response_body(
        &self,
        ctx: &mut RequestContext,
        _body: &[u8],
    ) -> Result<(), Error> {
        report_digest(ctx)?;
        let digest = ctx.get_value::<String>().ok().cloned().unwrap_or_default();
        uncache_request(&digest);
        ctx.continue_request()
    }

    async fn process_response_trailers(
        &self,
        ctx: &mut RequestContext,
        _trailers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.continue_request()
    }

    async fn error_handler(&self, _ctx: &mut RequestContext, phase: RequestPhase, err: Error) {
        tracing::error!("Error in phase {:?}: {:?}", phase, err);
    }

    fn close(&self, _grace_period_seconds: i32) -> Result<(), Error> {
        Ok(())
    }
}
