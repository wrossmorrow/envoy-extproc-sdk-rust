//! gRPC health server for the ext_proc server, wrapping `tonic-health`.

use crate::Error;
use tonic_health::pb::health_server::HealthServer as TonicHealthServer;
use tonic_health::server::HealthReporter;
use tonic_health::server::HealthService;
use tonic_health::ServingStatus;

/// Health status reported by the [`HealthServer`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HealthStatus {
    Unknown,
    Serving,
    NotServing,
}

impl From<HealthStatus> for ServingStatus {
    fn from(s: HealthStatus) -> ServingStatus {
        match s {
            HealthStatus::Unknown => ServingStatus::Unknown,
            HealthStatus::Serving => ServingStatus::Serving,
            HealthStatus::NotServing => ServingStatus::NotServing,
        }
    }
}

impl From<ServingStatus> for HealthStatus {
    fn from(s: ServingStatus) -> HealthStatus {
        match s {
            ServingStatus::Unknown => HealthStatus::Unknown,
            ServingStatus::Serving => HealthStatus::Serving,
            ServingStatus::NotServing => HealthStatus::NotServing,
        }
    }
}

/// Health server for the ext_proc gRPC service, implementing the standard
/// `grpc.health.v1` health checking service.
#[derive(Clone, Debug)]
pub struct HealthServer {
    reporter: HealthReporter,
    status: HealthStatus,
}

impl HealthServer {
    /// Create a health server reporting `NotServing`.
    pub fn new() -> Self {
        Self {
            reporter: HealthReporter::new(),
            status: HealthStatus::NotServing,
        }
    }

    /// Create a health server reporting `Serving`.
    pub fn new_ready() -> Self {
        Self {
            reporter: HealthReporter::new(),
            status: HealthStatus::Serving,
        }
    }

    /// Create a health server reporting the given status.
    pub fn with_status(status: HealthStatus) -> Self {
        Self {
            reporter: HealthReporter::new(),
            status,
        }
    }

    /// The current status of the server.
    pub fn get_status(&self) -> HealthStatus {
        self.status
    }

    /// Mark the server as ready (serving).
    pub async fn mark_ready(&mut self) {
        self.reporter
            .set_serving::<TonicHealthServer<HealthService>>()
            .await;
        self.status = HealthStatus::Serving;
    }

    /// Mark the server as not ready (not serving).
    pub async fn mark_not_ready(&mut self) {
        self.reporter
            .set_not_serving::<TonicHealthServer<HealthService>>()
            .await;
        self.status = HealthStatus::NotServing;
    }

    /// Check the serving status. Returns the current status.
    pub fn check(&self) -> HealthStatus {
        self.status
    }

    /// Watch the serving status. Not implemented by this SDK; the standard
    /// `grpc.health.v1` watch method is served by the underlying tonic-health
    /// service. Always returns an "unimplemented" error here.
    pub fn watch(&self) -> Result<(), Error> {
        Err("Watch is not implemented".into())
    }

    /// The tonic health service backing this [`HealthServer`].
    pub fn health_service(&self) -> TonicHealthServer<HealthService> {
        TonicHealthServer::new(HealthService::from_health_reporter(self.reporter.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_server_not_ready_by_default() {
        let hs = HealthServer::new();
        assert_eq!(hs.get_status(), HealthStatus::NotServing);
    }

    #[tokio::test]
    async fn test_health_server_ready_by_default() {
        let hs = HealthServer::new_ready();
        assert_eq!(hs.get_status(), HealthStatus::Serving);
    }

    #[tokio::test]
    async fn test_health_server_check() {
        let mut hs = HealthServer::new();
        assert_eq!(hs.check(), HealthStatus::NotServing);
        hs.mark_ready().await;
        assert_eq!(hs.check(), HealthStatus::Serving);
        hs.mark_not_ready().await;
        assert_eq!(hs.check(), HealthStatus::NotServing);
    }

    #[test]
    fn test_health_server_watch_is_unimplemented() {
        let hs = HealthServer::new();
        assert!(hs.watch().is_err());
    }
}
