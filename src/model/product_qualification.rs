//! Exact-output Product volume qualification.
//!
//! This ledger owns only qualification metadata. Product admission, native
//! transport authority, and the complete committed quantum remain independent.

use crate::protocol::OffsetRange;

/// Non-repeating identity of one exact attachment's qualification authority.
///
/// The value is intentionally opaque. Runtime flight records carry the receipt
/// returned by [`ProductQualificationLedger::tag_admitted_original`] instead of
/// reconstructing authority from a range or a path identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ProductQualificationEpoch(u64);

/// Receipt for the only range that may later move from `M` to `V`.
///
/// Receipts are copyable because ACK coverage can split one admitted range.
/// Copies remain safe: the ledger's normalized outstanding set makes every
/// byte release idempotent, and the epoch fences predecessor generations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "discarding the receipt discards exact qualification authority"]
pub(crate) struct ProductQualificationReceipt {
    epoch: ProductQualificationEpoch,
    tagged_range: OffsetRange,
}

impl ProductQualificationReceipt {
    #[cfg(test)]
    pub(crate) fn tagged_range(self) -> OffsetRange {
        self.tagged_range
    }

    /// Restricts authority to the intersection with an ACK or ambiguity range.
    pub(crate) fn intersect(self, range: OffsetRange) -> Option<Self> {
        OffsetRange::new(
            self.tagged_range.start.max(range.start),
            self.tagged_range.end.min(range.end),
        )
        .map(|tagged_range| Self {
            epoch: self.epoch,
            tagged_range,
        })
    }
}

/// Whether the ledger can mint receipts for one exact attachment generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProductQualificationAuthority {
    /// Admission is permitted; an evidence generation may not exist yet.
    Active,
    /// Admission is forbidden until exact requalification reactivates it.
    Revoked,
    /// The epoch counter cannot advance, so future evidence fails closed.
    Exhausted,
}

/// Why an admitted OriginalData quantum could not be tagged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProductQualificationAdmissionError {
    InvalidFloor,
    InvalidMaxQuantum,
    QuantumExceedsMaximum,
    FrozenParametersMismatch,
    OverlapsOutstandingTag,
    AuthorityRevoked,
    EpochExhausted,
}

/// Frozen bounds of the optional evidence generation under one active epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProductQualificationGeneration {
    floor_bytes: u64,
    max_quantum_bytes: u64,
}

/// One active exact attachment generation's capped Product evidence.
///
/// `floor_bytes`, `verified_bytes`, and `outstanding_tag_bytes` are the model's
/// `F`, `V`, and `M`. `tags` is the normalized set `T` of half-open Data
/// Sequence ranges. A receipt is the sole authority to release a member of `T`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ProductQualificationLedger {
    epoch: ProductQualificationEpoch,
    authority: ProductQualificationAuthority,
    generation: Option<ProductQualificationGeneration>,
    verified_bytes: u64,
    outstanding_tag_bytes: u64,
    tags: Vec<OffsetRange>,
}

impl Default for ProductQualificationLedger {
    fn default() -> Self {
        Self {
            epoch: ProductQualificationEpoch(1),
            authority: ProductQualificationAuthority::Active,
            generation: None,
            verified_bytes: 0,
            outstanding_tag_bytes: 0,
            tags: Vec::new(),
        }
    }
}

/// Inspectable form of the qualification invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProductQualificationInvariant {
    pub(crate) epoch: ProductQualificationEpoch,
    pub(crate) authority: ProductQualificationAuthority,
    pub(crate) floor_bytes: Option<u64>,
    pub(crate) max_quantum_bytes: Option<u64>,
    pub(crate) verified_bytes: u64,
    pub(crate) outstanding_tag_bytes: u64,
    pub(crate) tagged_range_bytes: u64,
    pub(crate) tagged_range_count: usize,
    pub(crate) tags_are_normalized: bool,
}

impl ProductQualificationInvariant {
    /// Active: `|T| <= M <= F`, `V + M <= F`, and `F` is addressable by `Vec`.
    /// Revoked/exhausted states own no evidence.
    pub(crate) fn holds(self) -> bool {
        let empty = self.verified_bytes == 0
            && self.outstanding_tag_bytes == 0
            && self.tagged_range_bytes == 0
            && self.tagged_range_count == 0
            && self.tags_are_normalized;
        match self.authority {
            ProductQualificationAuthority::Active => {
                if self.epoch.0 == 0 {
                    return false;
                }
                match (self.floor_bytes, self.max_quantum_bytes) {
                    (Some(floor_bytes), Some(max_quantum_bytes)) => {
                        floor_bytes > 0
                            && max_quantum_bytes > 0
                            && usize::try_from(floor_bytes).is_ok()
                            && self.tags_are_normalized
                            && self.outstanding_tag_bytes == self.tagged_range_bytes
                            && u64::try_from(self.tagged_range_count)
                                .is_ok_and(|count| count <= self.outstanding_tag_bytes)
                            && self.outstanding_tag_bytes <= floor_bytes
                            && self
                                .verified_bytes
                                .checked_add(self.outstanding_tag_bytes)
                                .is_some_and(|covered_bytes| covered_bytes <= floor_bytes)
                    }
                    (None, None) => empty,
                    _ => false,
                }
            }
            ProductQualificationAuthority::Revoked => {
                self.floor_bytes.is_none() && self.max_quantum_bytes.is_none() && empty
            }
            ProductQualificationAuthority::Exhausted => {
                self.epoch.0 == u64::MAX
                    && self.floor_bytes.is_none()
                    && self.max_quantum_bytes.is_none()
                    && empty
            }
        }
    }
}

impl ProductQualificationLedger {
    /// Atomically freezes (if generation-less) and tags admitted OriginalData.
    ///
    /// Authority is independent: a revoked ledger cannot silently reactivate
    /// merely because traffic arrived. `F` and `Q` are frozen by the first
    /// successful call while active. Every admitted range obeys `0 < N <= Q`;
    /// therefore a final quantum with residual deficit `d < N` has an untagged
    /// surplus `N - d < Q`. Rejected calls leave all evidence unchanged.
    pub(crate) fn tag_admitted_original(
        &mut self,
        floor_bytes: u64,
        max_quantum_bytes: u64,
        range: OffsetRange,
    ) -> Result<Option<ProductQualificationReceipt>, ProductQualificationAdmissionError> {
        Self::validate_parameters(floor_bytes, max_quantum_bytes, range)?;

        // Freshness is an admission premise even when the current deficit is
        // already zero. Check it before freezing a generation so every error
        // leaves the ledger byte-for-byte unchanged.
        if self
            .tags
            .iter()
            .any(|tag| tag.start < range.end && range.start < tag.end)
        {
            return Err(ProductQualificationAdmissionError::OverlapsOutstandingTag);
        }

        match self.authority {
            ProductQualificationAuthority::Active => {}
            ProductQualificationAuthority::Revoked => {
                return Err(ProductQualificationAdmissionError::AuthorityRevoked);
            }
            ProductQualificationAuthority::Exhausted => {
                return Err(ProductQualificationAdmissionError::EpochExhausted);
            }
        }
        match self.generation {
            Some(generation)
                if generation.floor_bytes != floor_bytes
                    || generation.max_quantum_bytes != max_quantum_bytes =>
            {
                return Err(ProductQualificationAdmissionError::FrozenParametersMismatch);
            }
            Some(_) => {}
            None => {
                debug_assert_eq!(self.verified_bytes, 0);
                debug_assert_eq!(self.outstanding_tag_bytes, 0);
                debug_assert!(self.tags.is_empty());
                self.generation = Some(ProductQualificationGeneration {
                    floor_bytes,
                    max_quantum_bytes,
                });
            }
        }

        let Some(deficit_bytes) = self.deficit_bytes() else {
            unreachable!("the validated generation is active")
        };
        let tag_bytes = deficit_bytes.min(range.len());
        if tag_bytes == 0 {
            debug_assert!(self.invariant().holds());
            return Ok(None);
        }
        let tagged_range = OffsetRange {
            start: range.start,
            // `tag_bytes <= range.len()` proves this cannot exceed `range.end`.
            end: range
                .start
                .checked_add(tag_bytes)
                .expect("a prefix of a valid OffsetRange cannot overflow"),
        };
        self.insert_normalized_tag(tagged_range);
        debug_assert!(self.invariant().holds());
        Ok(Some(ProductQualificationReceipt {
            epoch: self.epoch,
            tagged_range,
        }))
    }

    /// Releases exact uniquely attributable Data-ACKed receipt coverage.
    ///
    /// Returned bytes move from `M` to `V`. Duplicate, disjoint, revoked, or
    /// predecessor-epoch receipts return zero and cannot mint evidence.
    pub(crate) fn release_exact(
        &mut self,
        receipt: ProductQualificationReceipt,
        event_range: OffsetRange,
    ) -> u64 {
        let Some(receipt) = receipt.intersect(event_range) else {
            return 0;
        };
        if !self.receipt_is_current(receipt) {
            return 0;
        }
        let released = self.remove_tag_coverage(receipt.tagged_range);
        self.verified_bytes = self
            .verified_bytes
            .checked_add(released)
            .expect("qualification evidence is capped by its positive floor");
        debug_assert!(self.invariant().holds());
        released
    }

    /// Removes receipt coverage that cannot prove unique generation delivery.
    ///
    /// Ambiguity, accepted reinjection, and cleanup reduce `M` without advancing
    /// `V`. Only a receipt can name evidence; a bare DSN range has no authority.
    pub(crate) fn release_ambiguous(
        &mut self,
        receipt: ProductQualificationReceipt,
        event_range: OffsetRange,
    ) -> u64 {
        let Some(receipt) = receipt.intersect(event_range) else {
            return 0;
        };
        if !self.receipt_is_current(receipt) {
            return 0;
        }
        let released = self.remove_tag_coverage(receipt.tagged_range);
        debug_assert!(self.invariant().holds());
        released
    }

    /// Revokes admission at one exact lifecycle boundary and advances identity.
    ///
    /// The epoch advances even when no evidence generation was ever frozen;
    /// this prevents the active-without-generation lifecycle ABA. Duplicate
    /// revocation is a no-op rather than a heartbeat-driven epoch advance.
    pub(crate) fn revoke(&mut self) {
        if self.authority != ProductQualificationAuthority::Active {
            return;
        }
        let Some(next_epoch) = self.epoch.0.checked_add(1) else {
            self.revoke_evidence(ProductQualificationAuthority::Exhausted);
            debug_assert!(self.invariant().holds());
            return;
        };
        self.epoch = ProductQualificationEpoch(next_epoch);
        self.revoke_evidence(ProductQualificationAuthority::Revoked);
        debug_assert!(self.invariant().holds());
    }

    /// Restores admission after exact requalification, without minting evidence.
    pub(crate) fn reactivate_without_evidence(
        &mut self,
    ) -> Result<bool, ProductQualificationAdmissionError> {
        match self.authority {
            ProductQualificationAuthority::Active => Ok(false),
            ProductQualificationAuthority::Revoked => {
                debug_assert!(self.generation.is_none());
                debug_assert_eq!(self.verified_bytes, 0);
                debug_assert_eq!(self.outstanding_tag_bytes, 0);
                debug_assert!(self.tags.is_empty());
                self.authority = ProductQualificationAuthority::Active;
                debug_assert!(self.invariant().holds());
                Ok(true)
            }
            ProductQualificationAuthority::Exhausted => {
                Err(ProductQualificationAdmissionError::EpochExhausted)
            }
        }
    }

    /// Remaining tag volume for an active generation.
    pub(crate) fn deficit_bytes(&self) -> Option<u64> {
        if self.authority != ProductQualificationAuthority::Active {
            return None;
        }
        let floor_bytes = self.generation?.floor_bytes;
        Some(
            floor_bytes.saturating_sub(
                self.verified_bytes
                    .saturating_add(self.outstanding_tag_bytes),
            ),
        )
    }

    /// Durable assignment qualification derived only from exact tagged volume.
    pub(crate) fn qualified(&self) -> bool {
        self.authority == ProductQualificationAuthority::Active
            && self
                .generation
                .is_some_and(|generation| self.verified_bytes == generation.floor_bytes)
    }

    #[cfg(test)]
    pub(crate) fn epoch(&self) -> ProductQualificationEpoch {
        self.epoch
    }

    pub(crate) fn authority(&self) -> ProductQualificationAuthority {
        self.authority
    }

    pub(crate) fn invariant(&self) -> ProductQualificationInvariant {
        let tagged_range_bytes = self
            .tags
            .iter()
            .try_fold(0_u64, |total, range| total.checked_add(range.len()))
            .unwrap_or(u64::MAX);
        ProductQualificationInvariant {
            epoch: self.epoch,
            authority: self.authority,
            floor_bytes: self.generation.map(|generation| generation.floor_bytes),
            max_quantum_bytes: self
                .generation
                .map(|generation| generation.max_quantum_bytes),
            verified_bytes: self.verified_bytes,
            outstanding_tag_bytes: self.outstanding_tag_bytes,
            tagged_range_bytes,
            tagged_range_count: self.tags.len(),
            tags_are_normalized: self.tags.windows(2).all(|pair| {
                !pair[0].is_empty() && !pair[1].is_empty() && pair[0].end < pair[1].start
            }) && self.tags.first().is_none_or(|range| !range.is_empty()),
        }
    }

    fn validate_parameters(
        floor_bytes: u64,
        max_quantum_bytes: u64,
        range: OffsetRange,
    ) -> Result<(), ProductQualificationAdmissionError> {
        if floor_bytes == 0 || usize::try_from(floor_bytes).is_err() {
            return Err(ProductQualificationAdmissionError::InvalidFloor);
        }
        if max_quantum_bytes == 0 {
            return Err(ProductQualificationAdmissionError::InvalidMaxQuantum);
        }
        if range.is_empty() || range.len() > max_quantum_bytes {
            return Err(ProductQualificationAdmissionError::QuantumExceedsMaximum);
        }
        Ok(())
    }

    fn receipt_is_current(&self, receipt: ProductQualificationReceipt) -> bool {
        self.authority == ProductQualificationAuthority::Active && receipt.epoch == self.epoch
    }

    fn revoke_evidence(&mut self, authority: ProductQualificationAuthority) {
        debug_assert!(authority != ProductQualificationAuthority::Active);
        self.authority = authority;
        self.generation = None;
        self.verified_bytes = 0;
        self.outstanding_tag_bytes = 0;
        self.tags.clear();
    }

    fn insert_normalized_tag(&mut self, tag: OffsetRange) {
        debug_assert!(!tag.is_empty());
        let mut normalized = Vec::with_capacity(self.tags.len());
        let mut pending = tag;
        let mut inserted = false;
        for retained in self.tags.drain(..) {
            if retained.end < pending.start {
                normalized.push(retained);
            } else if pending.end < retained.start {
                if !inserted {
                    normalized.push(pending);
                    inserted = true;
                }
                normalized.push(retained);
            } else {
                pending.start = pending.start.min(retained.start);
                pending.end = pending.end.max(retained.end);
            }
        }
        if !inserted {
            normalized.push(pending);
        }
        self.tags = normalized;
        self.outstanding_tag_bytes = self
            .tags
            .iter()
            .try_fold(0_u64, |total, range| total.checked_add(range.len()))
            .expect("normalized tag volume is capped by an addressable floor");
    }

    fn remove_tag_coverage(&mut self, range: OffsetRange) -> u64 {
        if range.is_empty() || self.tags.is_empty() {
            return 0;
        }
        let mut retained = Vec::with_capacity(self.tags.len());
        let mut released = 0_u64;
        for tag in self.tags.drain(..) {
            let overlap_start = tag.start.max(range.start);
            let overlap_end = tag.end.min(range.end);
            if overlap_start >= overlap_end {
                retained.push(tag);
                continue;
            }
            released = released
                .checked_add(overlap_end - overlap_start)
                .expect("disjoint normalized tag coverage cannot overflow");
            if tag.start < overlap_start {
                retained.push(OffsetRange {
                    start: tag.start,
                    end: overlap_start,
                });
            }
            if overlap_end < tag.end {
                retained.push(OffsetRange {
                    start: overlap_end,
                    end: tag.end,
                });
            }
        }
        self.tags = retained;
        self.outstanding_tag_bytes = self
            .outstanding_tag_bytes
            .checked_sub(released)
            .expect("released tag coverage is present in cached M");
        released
    }

    #[cfg(test)]
    fn active_at_epoch_for_test(epoch: u64) -> Self {
        Self {
            epoch: ProductQualificationEpoch(epoch),
            ..Self::default()
        }
    }
}

#[cfg(test)]
#[path = "tests_product_qualification.rs"]
mod tests;
