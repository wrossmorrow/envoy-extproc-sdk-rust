//! Binary entry point for the extproc example server, for running the SDK
//! examples against a running `envoy`.

mod processors;

use std::sync::Arc;

use extproc::{
    Error, GenericExtProcServer, Metrics, ProcessingOptions, RequestProcessor, ServerOptions,
};

fn build_processor(
    name: &str,
    opts: ProcessingOptions,
) -> Result<Arc<dyn RequestProcessor>, Error> {
    match name {
        "noop" => Ok(Arc::new(processors::noop::NoopRequestProcessor { opts })),
        _ => Err(format!("processor {} not defined", name).into()),
    }
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("extproc server failed: {}", e);
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Error> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("Passing a processor is required.");
        std::process::exit(1);
    }
    let cmd = args.remove(0);
    let opts = ProcessingOptions::default();
    let processor = build_processor(&cmd, opts)?;
    let name = processor.get_name();
    let options = processor.get_options().clone();
    let server = GenericExtProcServer::new(name, processor, Some(options), Metrics::new());
    server.serve(ServerOptions::default()).await
}
