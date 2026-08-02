use super::*;
use crate::platform::{
    AddressFamily, DnsCaptureConfig, ProcessHostMutationBackend, ProcessHostOperation,
    ProcessNativeRoute, ProcessVpnEnvironment, RouteMode,
};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq)]
struct InjectedError(&'static str);

impl fmt::Display for InjectedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

struct InjectedBackend {
    failures: Arc<Mutex<HashMap<usize, usize>>>,
    next_token: usize,
}

impl ProcessHostMutationBackend for InjectedBackend {
    type RollbackToken = usize;
    type Error = InjectedError;

    fn apply(
        &mut self,
        _operation: &ProcessHostOperation,
    ) -> Result<Self::RollbackToken, Self::Error> {
        let token = self.next_token;
        self.next_token += 1;
        if token == 1 {
            return Err(InjectedError("apply"));
        }
        Ok(token)
    }

    fn rollback(
        &mut self,
        _operation: &ProcessHostOperation,
        token: &Self::RollbackToken,
    ) -> Result<(), Self::Error> {
        let mut failures = self.failures.lock().expect("fault control");
        let remaining = failures.entry(*token).or_insert(1);
        if *remaining > 0 {
            *remaining -= 1;
            return Err(InjectedError("rollback"));
        }
        Ok(())
    }
}

fn fault_plan() -> ProcessVpnPlan {
    let native = ProcessNativeRoute::new(AddressFamily::Ipv4, 7, None, 10).expect("native default");
    let environment = ProcessVpnEnvironment::new([native], vec![]).expect("native environment");
    let managed = ManagedVpnConfig::new(
        vec!["10.88.0.1/24".parse().expect("address")],
        1500,
        RouteMode::Full,
    )
    .expect("managed config")
    .with_dns(DnsCaptureConfig::new(vec!["10.88.0.53".parse().expect("DNS")]).expect("DNS config"))
    .expect("managed DNS");
    ProcessVpnPlan::build(
        &managed,
        &environment,
        12,
        ["198.51.100.10".parse().expect("carrier")],
        ["1.1.1.1".parse().expect("bootstrap DNS")],
    )
    .expect("plan")
}

#[test]
fn failed_prepare_retries_residual_reverse_before_returning() {
    let failures = Arc::new(Mutex::new(HashMap::new()));
    let backend = InjectedBackend {
        failures: failures.clone(),
        next_token: 0,
    };
    let error = match WindowsVpnHostLifecycle::prepare(backend, fault_plan()) {
        Ok(_) => panic!("injected prepare failure"),
        Err(error) => error,
    };

    assert!(error.cleanup.is_none());
    assert_eq!(
        failures.lock().expect("fault control").get(&0).copied(),
        Some(0),
        "residual rollback was not retried"
    );
}

#[test]
fn generated_wintun_identity_is_nonzero_and_rfc4122_shaped() {
    let guid = random_generation_guid().expect("random GUID");
    let bytes = guid.to_be_bytes();
    assert_ne!(guid, 0);
    assert_eq!(bytes[6] >> 4, 4);
    assert_eq!(bytes[8] >> 6, 2);
}
