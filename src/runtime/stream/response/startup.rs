//! Exact server-side state for one requester's frozen return-topology plan.
//!
//! This state is not a scheduler and owns no Product credit. Its only sender
//! effect is the finite, non-refilling unique-offset ceiling exposed by
//! `fresh_data_limit` until one exact FINAL becomes absorbing.

use super::{ResponseAcquisitionOutputId, ResponseStreamBinding};
use crate::model::capacity::{MIN_RELIABLE_PIPE_PACKETS, PATH_OPEN_SCORE_BYTES};
use crate::protocol::{PathUsage, StreamAttachmentPhase, StreamReturnPlan};
use crate::runtime::RuntimeError;
use std::collections::BTreeMap;

const fn response_startup_core_trigger_bytes() -> u64 {
    (MIN_RELIABLE_PIPE_PACKETS * PATH_OPEN_SCORE_BYTES) as u64
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) enum ResponseStartupFinalOutcome {
    Finalized {
        /// Exact enrolled outputs atomically withdrawn from new Product
        /// placement by this first FINAL. The stream actor must consume one
        /// ordered detach event for each identity so already-published input
        /// and retained flights keep their ordinary ordering boundary.
        withdrawn_outputs: Vec<ResponseAcquisitionOutputId>,
    },
    Duplicate,
}

#[derive(Debug)]
enum ResponseStartupFinalCommit {
    Duplicate,
    Finalize {
        next: ResponseStartupPlanPhase,
        omitted_bindings: Vec<ResponseAcquisitionOutputId>,
    },
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ResponseStartupAttachmentCommit {
    binding: Option<(u8, ResponseAcquisitionOutputId)>,
}

#[derive(Debug)]
enum ResponseStartupPlanPhase {
    /// A one-member selected tier with `total=1,h=0` is ready immediately.
    Singleton {
        opening: ResponseAcquisitionOutputId,
        candidate_tier: PathUsage,
    },
    Unresolved {
        trigger_bytes: u64,
        candidate_total: u8,
        candidate_tier: PathUsage,
        bindings: BTreeMap<u8, ResponseAcquisitionOutputId>,
    },
    Finalized {
        trigger_bytes: u64,
        candidate_total: u8,
        candidate_tier: PathUsage,
        retained_ordinals: Vec<u8>,
        _retained_bindings: BTreeMap<u8, ResponseAcquisitionOutputId>,
    },
}

#[derive(Debug)]
pub(super) struct ResponseStartupPlanState {
    phase: ResponseStartupPlanPhase,
}

impl ResponseStartupPlanState {
    pub(super) fn from_initial_open(
        plan: StreamReturnPlan,
        opening: ResponseAcquisitionOutputId,
    ) -> Result<Self, RuntimeError> {
        validate_return_plan_shape(plan)?;
        if plan.phase != StreamAttachmentPhase::Startup {
            return Err(RuntimeError::Protocol(
                "initial stream attachment must enroll in its return plan",
            ));
        }
        if plan.candidate_total == 1 {
            if plan.trigger_bytes != 0 || plan.candidate_ordinal != 0 {
                return Err(RuntimeError::Protocol(
                    "single-output return plan is not canonical",
                ));
            }
            return Ok(Self {
                phase: ResponseStartupPlanPhase::Singleton {
                    opening,
                    candidate_tier: plan.candidate_tier,
                },
            });
        }
        if plan.trigger_bytes == 0 || plan.trigger_bytes > response_startup_core_trigger_bytes() {
            return Err(RuntimeError::Protocol(
                "multipath return trigger is outside the core startup bound",
            ));
        }
        let mut bindings = BTreeMap::new();
        bindings.insert(plan.candidate_ordinal, opening);
        Ok(Self {
            phase: ResponseStartupPlanPhase::Unresolved {
                trigger_bytes: plan.trigger_bytes,
                candidate_total: plan.candidate_total,
                candidate_tier: plan.candidate_tier,
                bindings,
            },
        })
    }

    /// Validates one attachment while the caller holds the response-output
    /// lock, returning an infallible state mutation to commit before unlock.
    /// ORDINARY attachments preserve recovery before FINAL but never enroll,
    /// settle, or otherwise mutate the frozen startup plan.
    pub(super) fn prepare_attachment(
        &self,
        plan: StreamReturnPlan,
        exact: ResponseAcquisitionOutputId,
    ) -> Result<ResponseStartupAttachmentCommit, RuntimeError> {
        validate_return_plan_shape(plan)?;
        match &self.phase {
            ResponseStartupPlanPhase::Singleton {
                opening,
                candidate_tier,
            } => {
                validate_signature(plan, 0, 1, *candidate_tier)?;
                match plan.phase {
                    StreamAttachmentPhase::Ordinary => {
                        Ok(ResponseStartupAttachmentCommit { binding: None })
                    }
                    StreamAttachmentPhase::Startup if plan.candidate_ordinal == 0 => {
                        if *opening != exact {
                            return Err(RuntimeError::Protocol(
                                "return-plan ordinal reused by another exact attachment",
                            ));
                        }
                        Ok(ResponseStartupAttachmentCommit { binding: None })
                    }
                    StreamAttachmentPhase::Startup => unreachable!("shape checked ordinal"),
                }
            }
            ResponseStartupPlanPhase::Unresolved {
                trigger_bytes,
                candidate_total,
                candidate_tier,
                bindings,
            } => {
                validate_signature(plan, *trigger_bytes, *candidate_total, *candidate_tier)?;
                if plan.phase == StreamAttachmentPhase::Ordinary {
                    return Ok(ResponseStartupAttachmentCommit { binding: None });
                }
                if let Some(bound) = bindings.get(&plan.candidate_ordinal) {
                    if *bound != exact {
                        return Err(RuntimeError::Protocol(
                            "return-plan ordinal reused by another exact attachment",
                        ));
                    }
                    return Ok(ResponseStartupAttachmentCommit { binding: None });
                }
                if bindings.values().any(|bound| *bound == exact) {
                    return Err(RuntimeError::Protocol(
                        "return-plan exact attachment reused by another ordinal",
                    ));
                }
                Ok(ResponseStartupAttachmentCommit {
                    binding: Some((plan.candidate_ordinal, exact)),
                })
            }
            ResponseStartupPlanPhase::Finalized {
                trigger_bytes,
                candidate_total,
                candidate_tier,
                retained_ordinals: _,
                _retained_bindings: _,
            } => {
                validate_signature(plan, *trigger_bytes, *candidate_total, *candidate_tier)?;
                if plan.phase == StreamAttachmentPhase::Startup {
                    return Err(RuntimeError::Protocol(
                        "startup attachment arrived after return-plan finalization",
                    ));
                }
                Ok(ResponseStartupAttachmentCommit { binding: None })
            }
        }
    }

    pub(super) fn commit_attachment(&mut self, commit: ResponseStartupAttachmentCommit) {
        let Some((ordinal, exact)) = commit.binding else {
            return;
        };
        let ResponseStartupPlanPhase::Unresolved { bindings, .. } = &mut self.phase else {
            unreachable!("validated startup attachment changed phase before commit");
        };
        let previous = bindings.insert(ordinal, exact);
        debug_assert!(previous.is_none());
    }

    pub(super) fn fresh_data_limit(
        &self,
        next_offset: u64,
        proposed_bytes: usize,
    ) -> Option<usize> {
        if proposed_bytes == 0 {
            return None;
        }
        let ResponseStartupPlanPhase::Unresolved { trigger_bytes, .. } = &self.phase else {
            return Some(proposed_bytes);
        };
        let remaining = trigger_bytes.checked_sub(next_offset)?;
        if remaining == 0 {
            return None;
        }
        Some(proposed_bytes.min(usize::try_from(remaining).unwrap_or(usize::MAX)))
    }

    fn prepare_finalization(
        &self,
        retained_ordinals: &[u8],
    ) -> Result<ResponseStartupFinalCommit, RuntimeError> {
        if retained_ordinals.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(RuntimeError::Protocol(
                "return-plan final ordinals are not strictly increasing",
            ));
        }
        match &self.phase {
            ResponseStartupPlanPhase::Singleton { .. } => Err(RuntimeError::Protocol(
                "canonical singleton return plan is already ready",
            )),
            ResponseStartupPlanPhase::Finalized {
                retained_ordinals: accepted,
                ..
            } => {
                if accepted.as_slice() == retained_ordinals {
                    Ok(ResponseStartupFinalCommit::Duplicate)
                } else {
                    Err(RuntimeError::Protocol(
                        "return plan was finalized with different membership",
                    ))
                }
            }
            ResponseStartupPlanPhase::Unresolved {
                trigger_bytes,
                candidate_total,
                candidate_tier,
                bindings,
            } => {
                let mut retained_bindings = BTreeMap::new();
                for ordinal in retained_ordinals {
                    if *ordinal >= *candidate_total {
                        return Err(RuntimeError::Protocol(
                            "return-plan final ordinal is outside its declared total",
                        ));
                    }
                    let Some(exact) = bindings.get(ordinal).copied() else {
                        return Err(RuntimeError::Protocol(
                            "return-plan final retained an unenrolled ordinal",
                        ));
                    };
                    retained_bindings.insert(*ordinal, exact);
                }
                let omitted_bindings = bindings
                    .iter()
                    .filter_map(|(ordinal, exact)| {
                        (!retained_bindings.contains_key(ordinal)).then_some(*exact)
                    })
                    .collect();
                let next = ResponseStartupPlanPhase::Finalized {
                    trigger_bytes: *trigger_bytes,
                    candidate_total: *candidate_total,
                    candidate_tier: *candidate_tier,
                    retained_ordinals: retained_ordinals.to_vec(),
                    _retained_bindings: retained_bindings,
                };
                Ok(ResponseStartupFinalCommit::Finalize {
                    next,
                    omitted_bindings,
                })
            }
        }
    }

    fn commit_finalization(&mut self, next: ResponseStartupPlanPhase) {
        debug_assert!(matches!(
            self.phase,
            ResponseStartupPlanPhase::Unresolved { .. }
        ));
        debug_assert!(matches!(next, ResponseStartupPlanPhase::Finalized { .. }));
        self.phase = next;
    }

    #[cfg(test)]
    fn finalize_for_test(
        &mut self,
        retained_ordinals: &[u8],
    ) -> Result<ResponseStartupFinalOutcome, RuntimeError> {
        match self.prepare_finalization(retained_ordinals)? {
            ResponseStartupFinalCommit::Duplicate => Ok(ResponseStartupFinalOutcome::Duplicate),
            ResponseStartupFinalCommit::Finalize { next, .. } => {
                self.commit_finalization(next);
                Ok(ResponseStartupFinalOutcome::Finalized {
                    withdrawn_outputs: Vec::new(),
                })
            }
        }
    }
}

impl ResponseStreamBinding {
    /// Intersects a proposed fresh response-data quantum with the one-shot
    /// pre-FINAL unique-offset ceiling. ACK/recovery state is intentionally not
    /// an input, so the allowance cannot refill.
    pub(in crate::runtime) fn response_startup_fresh_data_limit(
        &self,
        next_offset: u64,
        proposed_bytes: usize,
    ) -> Option<usize> {
        self.response_startup
            .lock()
            .expect("server response startup lock")
            .fresh_data_limit(next_offset, proposed_bytes)
    }

    /// Accepts one exact FINAL. Membership is the historical enrollment
    /// transcript: detach or usage changes after peer acceptance cannot erase
    /// it or make an equal duplicate FINAL unequal.
    pub(in crate::runtime) fn finalize_response_startup_plan(
        &self,
        retained_ordinals: &[u8],
    ) -> Result<ResponseStartupFinalOutcome, RuntimeError> {
        let mut startup = self
            .response_startup
            .lock()
            .expect("server response startup lock");
        let prepared = startup.prepare_finalization(retained_ordinals)?;
        let ResponseStartupFinalCommit::Finalize {
            next,
            omitted_bindings,
        } = prepared
        else {
            return Ok(ResponseStartupFinalOutcome::Duplicate);
        };
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let mut withdrawn_outputs = Vec::new();
        for omitted in omitted_bindings {
            let Some(position) = outputs.entries.iter().position(|entry| {
                entry.key == omitted.key
                    && entry.path_instance_id == omitted.path_instance_id
                    && entry.incarnation == omitted.incarnation
            }) else {
                continue;
            };
            let mut entry = outputs.entries.remove(position);
            entry.load_registration.deactivate();
            entry.product_qualification.revoke();
            withdrawn_outputs.push(omitted);
            outputs.detaching.push(entry);
        }
        if !withdrawn_outputs.is_empty() {
            self.response_model_generation
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            self.output_membership_generation
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
        // Ready is published only after every omitted exact output has left
        // the schedulable set. A sender holding either lock can therefore see
        // neither an uncapped ghost output nor a capped finalized plan.
        startup.commit_finalization(next);
        drop(outputs);
        drop(startup);
        for omitted in &withdrawn_outputs {
            self.clear_request_feedback_ingress_if(omitted.key, omitted.path_instance_id);
        }
        // FINAL also removes the one-shot sender ceiling when every enrolled
        // output is retained (or was already detached), so it always wakes a
        // blocked service on the first transition.
        self.notify_update();
        Ok(ResponseStartupFinalOutcome::Finalized { withdrawn_outputs })
    }

    #[cfg(test)]
    pub(in crate::runtime) fn install_unresolved_response_startup_for_test(
        &self,
        trigger_bytes: u64,
        candidate_total: u8,
        candidate_tier: PathUsage,
        opening_key: crate::model::path::CarrierPathKey,
    ) {
        let opening = {
            let outputs = self
                .outputs
                .lock()
                .expect("server reliable stream binding lock");
            outputs
                .entries
                .iter()
                .find(|entry| entry.key == opening_key)
                .map(ResponseAcquisitionOutputId::from)
                .expect("test opening output")
        };
        let state = Self::test_startup_state(
            StreamReturnPlan {
                trigger_bytes,
                candidate_total,
                candidate_tier,
                phase: StreamAttachmentPhase::Startup,
                candidate_ordinal: 0,
            },
            opening,
        );
        *self
            .response_startup
            .lock()
            .expect("server response startup lock") = state;
    }

    #[cfg(test)]
    fn test_startup_state(
        plan: StreamReturnPlan,
        opening: ResponseAcquisitionOutputId,
    ) -> ResponseStartupPlanState {
        ResponseStartupPlanState::from_initial_open(plan, opening)
            .expect("valid test return startup plan")
    }
}

fn validate_return_plan_shape(plan: StreamReturnPlan) -> Result<(), RuntimeError> {
    if plan.candidate_total == 0 || plan.candidate_ordinal >= plan.candidate_total {
        return Err(RuntimeError::Protocol(
            "return-plan candidate ordinal is outside its declared total",
        ));
    }
    if plan.phase == StreamAttachmentPhase::Ordinary && plan.candidate_ordinal != 0 {
        return Err(RuntimeError::Protocol(
            "ordinary return-plan attachment must use canonical ordinal zero",
        ));
    }
    Ok(())
}

fn validate_signature(
    plan: StreamReturnPlan,
    trigger_bytes: u64,
    candidate_total: u8,
    candidate_tier: PathUsage,
) -> Result<(), RuntimeError> {
    if plan.trigger_bytes != trigger_bytes
        || plan.candidate_total != candidate_total
        || plan.candidate_tier != candidate_tier
    {
        return Err(RuntimeError::Protocol(
            "stream return-plan identity changed across attachments",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests_startup.rs"]
mod tests;
