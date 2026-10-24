use std::any::Any;
use std::sync::{Mutex, MutexGuard};

use extproc::{
    AllHeaders, Error, HeaderValue, ProcessingOptions, RequestContext, RequestProcessor,
};
use sha2::{Digest, Sha256};

pub struct DigestRequestProcessor {
    pub opts: ProcessingOptions,
}

fn lock_hasher(ctx: &RequestContext) -> Result<MutexGuard<'_, Sha256>, Error> {
    let value = ctx.get_value("hasher")?;
    let hasher = value
        .downcast_ref::<Mutex<Sha256>>()
        .ok_or_else(|| "invalid hasher".to_string())?;
    Ok(hasher
        .lock()
        .map_err(|_| "hasher lock failed".to_string())?)
}

fn compute_digest(hasher: &mut Sha256) -> String {
    hex::encode(hasher.clone().finalize())
}

fn set_digest(ctx: &mut RequestContext, digest: &str) -> Result<(), Error> {
    ctx.set_value(
        "digest",
        Box::new(digest.to_string()) as Box<dyn Any + Send + Sync>,
    )?;
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
        if let Some(digest) = ctx
            .get_value("digest")
            .ok()
            .and_then(|v| v.downcast_ref::<String>().cloned())
        {
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

impl RequestProcessor for DigestRequestProcessor {
    fn get_name(&self) -> String {
        "digest".to_string()
    }

    fn get_options(&self) -> &ProcessingOptions {
        &self.opts
    }

    fn process_request_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.set_value(
            "hasher",
            Box::new(Mutex::new(Sha256::new())) as Box<dyn Any + Send + Sync>,
        )?;
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
        }
        ctx.continue_request()
    }

    fn process_request_trailers(
        &self,
        ctx: &mut RequestContext,
        _trailers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.continue_request()
    }

    fn process_request_body(&self, ctx: &mut RequestContext, body: &[u8]) -> Result<(), Error> {
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
        }
        ctx.continue_request()
    }

    fn process_response_headers(
        &self,
        ctx: &mut RequestContext,
        _headers: &AllHeaders,
    ) -> Result<(), Error> {
        report_digest(ctx)?;
        ctx.continue_request()
    }

    fn process_response_body(&self, ctx: &mut RequestContext, _body: &[u8]) -> Result<(), Error> {
        report_digest(ctx)?;
        ctx.continue_request()
    }

    fn process_response_trailers(
        &self,
        ctx: &mut RequestContext,
        _trailers: &AllHeaders,
    ) -> Result<(), Error> {
        ctx.continue_request()
    }

    fn error_handler(&self, _ctx: &mut RequestContext, phase: u32, err: Error) {
        tracing::error!("Error in phase {}: {:?}", phase, err);
    }

    fn close(&self, _grace_period_seconds: i32) -> Result<(), Error> {
        Ok(())
    }
}
