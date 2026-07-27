# Performance impact declarations

`performance-impact-registry.json` is the versioned F0 contract that maps
performance-sensitive paths to minimum impact scopes, cells, profiles, and
metrics. A cell covers every profile listed in the registry; selecting one
convenient runner row does not satisfy it.

Inspect the minimum declaration for an authoritative changed-path list:

```bash
mkdir -p .tmp/performance
git diff --name-only origin/main...HEAD > .tmp/performance/changed-paths.txt
python3 lab/validate_performance_declaration.py \
  --changed-paths-file .tmp/performance/changed-paths.txt \
  --print-required
```

Validate a candidate declaration during review:

```bash
python3 lab/validate_performance_declaration.py \
  --changed-paths-file .tmp/performance/changed-paths.txt \
  --declaration lab/performance-change.example.json \
  --phase declaration
```

Validate acceptance only after complete evidence is attached:

```bash
python3 lab/validate_performance_declaration.py \
  --changed-paths-file .tmp/performance/changed-paths.txt \
  --declaration path/to/change.json \
  --phase acceptance
```

An acceptance record may split a cell across multiple evidence entries, but
every declared `(cell, metric)` pair must occur exactly once. Each entry must:

- use an acceptance-capable lane at or above the cell minimum;
- cover the cell's complete registered profile list;
- meet its minimum valid-pair and triggered-event counts;
- include every required comparison reference;
- retain checksummed raw and generated-summary artifacts;
- assert the registered completeness properties; and
- report `pass` or an approved latency/stability tradeoff record.

`quick` is deliberately marked `triage_only`. Its `clear` or `signal` result
only prioritizes the repeated run; it can neither accept nor veto a candidate.
Uncertain impact expands to `full-matrix`. Measurement-definition changes
also require a dual run.

Acceptance uses at least seven adjacent matched repeats and a preregistered
two-sided 95% paired-bootstrap interval for each normalized candidate delta.
An interval wholly above zero proves improvement, one wholly below zero proves
regression, and every overlap with zero is `INCONCLUSIVE`. `INCONCLUSIVE`
evidence is rerun and is never promoted, persisted, or used to trigger a
revert. `PASS` requires at least one proven intended improvement and no proven
regression; exact all-zero deterministic equivalence may also pass. Champion
promotion additionally requires a proven improvement over the champion.

A proven regression fails unless a preregistered, owner-approved theoretical
latency/stability tradeoff is supported by Pareto and ablation evidence. The
approval records the observed normalized cost; there is no universal
percentage, absolute noninferiority margin, or maximum-regression cap.

The validator does not execute experiments or trust a summary as raw evidence.
CI must supply its own authoritative changed-path list. The evidence
runner/importer must produce the digests, coverage, statistical result, and
comparison identities consumed here; binding those records to actual result
artifacts is a separate F0 integration step.
