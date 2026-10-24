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
        "trivial" => Ok(Arc::new(processors::trivial::TrivialRequestProcessor {
            opts,
        })),
        "timer" => Ok(Arc::new(processors::timer::TimerRequestProcessor { opts })),
        "data" => Ok(Arc::new(processors::data::DataRequestProcessor { opts })),
        "digest" => Ok(Arc::new(processors::digest::DigestRequestProcessor {
            opts,
        })),
        "dedup" => Ok(Arc::new(processors::dedup::DedupRequestProcessor { opts })),
        "masker" => Ok(Arc::new(processors::masker::MaskerRequestProcessor::new(
            opts,
        ))),
        "echo" => Ok(Arc::new(processors::echo::EchoRequestProcessor { opts })),
        "router" => Ok(Arc::new(processors::router::RoutingRequestProcessor {
            opts,
        })),
        _ => Err(format!("processor {} not defined", name).into()),
    }
}

/// Parse the post-processor arguments: `-port <n>`, `-update-extproc-header`,
/// `-update-duration-header`. Flags may also use `--` or `=`.
fn parse_flags(args: &[String]) -> (u16, bool, bool) {
    let mut port = 50051u16;
    let mut update_ext_proc_header = false;
    let mut update_duration_header = false;
    let mut i = 0;
    while i < args.len() {
        let key = args[i].trim_start_matches('-');
        if key == "port" {
            if let Some(v) = args.get(i + 1) {
                port = v.parse().unwrap_or(port);
            }
            i += 2;
        } else if key == "update-extproc-header" {
            update_ext_proc_header = true;
            i += 1;
        } else if key == "update-duration-header" {
            update_duration_header = true;
            i += 1;
        } else {
            i += 1;
        }
    }
    (port, update_ext_proc_header, update_duration_header)
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
    let (port, update_ext_proc_header, update_duration_header) = parse_flags(&args);
    let server_options = ServerOptions {
        ext_proc_port: port,
        unready_propagation_delay_seconds: 0,
        termination_grace_period_seconds: 1,
        ..Default::default()
    };
    let opts = ProcessingOptions {
        update_ext_proc_header,
        update_duration_header,
        ..Default::default()
    };
    let processor = build_processor(&cmd, opts)?;
    let name = processor.name();
    let options = processor.options().clone();
    let server = GenericExtProcServer::new(name, processor, Some(options), Metrics::new());
    server.serve(server_options).await
}
