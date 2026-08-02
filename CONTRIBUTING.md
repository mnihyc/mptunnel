# Contributing

`mptunnel` is a networking product with a custom protocol. Changes must preserve the
ownership boundary described by [RFC.md](RFC.md) and
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): MPP owns data-level sequencing,
flow control, scheduling, and reinjection; TCP and QUIC retain native congestion
control and loss recovery.

Before changing protocol behavior or scheduling, open an issue that states the
interoperability or measured runtime problem. Performance changes need a
reproducible matched case and a causal explanation; a simulator result alone is
not sufficient.

## Local checks

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
python3 -m unittest discover --start-directory lab --pattern 'test_*.py'
```

Keep production modules aligned with the dependency and ownership boundaries
in [the architecture](docs/ARCHITECTURE.md). Put Rust unit tests in sibling
`tests_<owner>.rs` files or a dedicated test directory, and avoid case-specific
constants or platform identity checks when a measured capability is the real
distinction.

Lab changes must preserve namespace isolation, receiver-confirmed upload
accounting, binary/config identity, and explicit incomplete-result handling.

By contributing, you agree that your contribution is licensed under the
[Apache License 2.0](LICENSE).
