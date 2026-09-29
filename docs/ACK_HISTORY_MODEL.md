# Response ACK ordering and retired-copy history

Dynamic response ownership distinguishes exact active attempts from historical
coverage. Wire acknowledgements, recovery permission, native congestion control
and unique Product-byte limits have separate owners. Request and fixed-response
ledgers retain their additive debt semantics.

## Three distinct lifetimes

1. Current and detaching physical attempts retain their exact identity, time,
   qualification, range and debt fields.
2. A finally removed, non-proving ReinjectedData attempt with no outstanding
   qualification receipt has historical coverage but no current service authority.
3. After authoritative Product ACK, a non-proving copy in ACK-ordering history
   contributes only to closing the ordered coverage frontier.

Output staleness and elapsed native recovery time are not final retirement.
Only final attachment replacement and completed detach take the retirement path,
under the existing outputs-to-flights lock order and generation fence. Original
records remain exact. Exact output incarnations cannot be reused.
Readers that release the membership lock before acquiring the flight lock still
produce advisory observations; existing exact-instance, generation, range and slot
validation at publication remains mandatory. An advisory snapshot can contain
earlier identities; final admission must revalidate them. Historical witnesses are not an inventory of every attempt.

## Pre-ACK representation

Within one equal-start bucket, historical copy intervals are nested. Their union
is the interval ending at the largest endpoint. Keep one actual max-end retired
copy as its witness, plus all Original, current, detaching and qualification-bearing
records. Do not synthesize path authority and do not turn a witness into a live
publication. Current-owner debt, recovery suppression and path evidence continue
to filter by exact active identity.

The existing invalidation pass also forms witnesses. No second whole-ledger
retirement scan is added. Rebuild only changed identity buckets. When a bucket
loses rows and its capacity exceeds twice its retained length,
request capacity for twice that length. This leaves room for replacement appends
without reallocating after every retirement. This geometric storage rule does
not limit accepted attempts; the allocator may retain additional capacity.

**Do not recompute U/M from compacted physical rows.** U is the retained
covered-byte union and M is retained historical ambiguity. Compaction changes
neither; Product ACK still subtracts its authoritative mask from both. Keep the
Indexed lifetime even when its physical count falls to one. This preserves
copy-ever exclusion and exact partial-ACK ambiguity without adding another
persistent history union.

For the eligible class, repeated incarnations at one start retain at most one
retired witness. This is not a new hard bound on all metadata: distinct starts,
Originals, protected owners, future receipt-bearing copies and allocator overhead
remain. Physical-attempt enumeration after final retirement becomes historical
coverage enumeration; it is not a physical transmission counter.

## Post-ACK representation and work

Ordering compaction does not modify the release vector passed into the ordering
transaction. Earlier final-retirement compaction can already have reduced its
non-proving retired-copy rows; Original releases and current/detaching accounting
remain exact. Non-proving ordering copies at equal starts use one max-end witness.
Proving and Original records keep their original order.
Consumers of newly contiguous records cannot use non-proving counts as physical
attempt telemetry. The `released_history_rows` diagnostic counts the retained
release records, including witnesses, rather than every physical attempt.

Frontier closure merges sorted ACK masks and sorted hole starts once. The change
in latest-Original hole volume comes from only this transaction's changed buckets,
not from two global totals. An ACK that
cannot reach the minimum stored start skips the unrelated retain traversal.
Diagnostic builds still compute the full reported total.

When history is empty and an ordered batch of Original releases is wholly
covered by the reached frontier, emit releases directly. Preserve the reference
order: already-contiguous releases first, followed by newly closed releases.
No temporary BTree map is required. Other shapes take the general path.

## Invariants

- Original and proving output records retain their metadata and order.
- Current, detaching and qualification-bearing attempts retain exact identities.
- Compaction preserves covered bytes, historical ambiguity and copy exclusion.
- Partial ACK fragments retain ownership without becoming new publications.
- Empty/reset transitions clear history; one remaining witness does not erase M.
- Staleness alone does not authorize compaction, and retirement does not ACK data.
