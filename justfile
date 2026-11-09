# Show available development commands.
default:
    @just --list

# Check formatting, run strict Clippy, then run the test suite.
ci: fmt-check clippy test

# Type-check every target with the committed dependency lockfile.
check:
    cargo check --locked --all-targets

# Format Rust sources.
fmt:
    cargo fmt --all

# Check Rust formatting without changing files.
fmt-check:
    cargo fmt --all -- --check

# Reject Clippy warnings in all targets.
clippy:
    cargo clippy --locked --all-targets -- -D warnings

# Run unit, integration and documentation tests.
test:
    cargo test --locked

# Build the debug executable.
build:
    cargo build --locked

# Build the release executable.
release:
    cargo build --locked --release
