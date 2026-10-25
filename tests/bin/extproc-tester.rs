//! Binary entry point for the extproc server backed by the [`TesterRequestProcessor`],
//! for running the integration tests against a real server process.

use std::sync::Arc;

use extproc::{
    GenericExtProcServer, Metrics, ProcessingOptions, ServerOptions, TesterRequestProcessor,
};

#[tokio::main]
async fn main() {
    let mut server_options = ServerOptions::new();
    // Optional CLI args: --extproc-port <port> --metrics-http-port <port>
    //                    --update-duration-header
    let mut args = std::env::args().skip(1);
    let mut update_duration_header = false;
    while let Some(arg) = args.next() {
        if arg == "--extproc-port" {
            if let Some(v) = args.next() {
                server_options.ext_proc_port = v.parse().unwrap();
            }
        } else if arg == "--metrics-http-port" {
            if let Some(v) = args.next() {
                server_options.metrics_http_port = v.parse().unwrap();
            }
        } else if arg == "--update-duration-header" {
            update_duration_header = true;
        }
    }
    let processor = Arc::new(TesterRequestProcessor::default());
    let options = ProcessingOptions {
        update_duration_header,
        ..Default::default()
    };
    let server = GenericExtProcServer::new(
        "tester".to_string(),
        processor,
        Some(options),
        Metrics::new(),
    );
    if let Err(e) = server.serve(server_options).await {
        eprintln!("extproc tester server failed: {}", e);
        std::process::exit(1);
    }
}
