# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

### Full Build (via unified script)
```bash
# Development build (faster, less optimized)
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12

# Release build (optimized)
./auron-build.sh --release --sparkver 3.5 --scalaver 2.12

# Skip native Rust build (Java/Scala only changes)
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 -DskipBuildNative

# Docker build (for reproducible builds)
./auron-build.sh --docker true --image centos7 --release --sparkver 3.5 --scalaver 2.12

# With optional integrations
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --celeborn 0.6 --uniffle 0.10 --paimon 1.2
```

Run `./auron-build.sh --help` for all options.

### Individual Components
```bash
# Run Rust tests
cargo test

# Run Scala tests for specific module
./build/mvn test -pl spark-extension -Pspark-3.5 -Pscala-2.12

# Run all Scala tests
./build/mvn test -Pspark-3.5 -Pscala-2.12

# Run Spark integration tests
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --sparktests true
```

### Code Formatting
```bash
# Format all code (Java, Scala, Rust)
./dev/reformat

# Check formatting without changes
./dev/reformat --check
```

## Architecture Overview

Auron is a native accelerator for big data engines (Spark, Flink) that uses Rust-based vectorized execution via Apache DataFusion. The system has two main layers:

### JVM Layer (Java/Scala)
- **spark-extension/**: Spark integration via `AuronSparkSessionExtension` - injects columnar rules that convert Spark physical plans to native execution
- **spark-extension-shims-spark/**: Spark version-specific implementations (shims pattern for Spark 3.0-4.1 compatibility)
- **auron-core/**: Core Java classes including `JniBridge` for native calls, configuration, and resource management
- **common/**: Shared utilities

### Native Layer (Rust)
Located in `native-engine/`:
- **auron/**: Main entry point, task execution, panic handling
- **auron-jni-bridge/**: JNI bridge macros and Java class bindings
- **auron-planner/**: Query plan translation from protobuf to DataFusion plans
- **auron-memmgr/**: Native memory management
- **datafusion-ext-plans/**: Native execution operators (scan, join, agg, sort, shuffle, etc.)
- **datafusion-ext-exprs/**: Expression evaluation
- **datafusion-ext-functions/**: Scalar and aggregate functions
- **datafusion-ext-commons/**: Shared utilities

### Plan Serialization Flow
1. Spark physical plan → `AuronConverters` determines convertible operators
2. Plan serialized to protobuf via `auron.proto` (see `dev/mvn-build-helper/proto/target/classes/auron.proto`)
3. Native side deserializes and translates to DataFusion execution plan
4. Native execution via DataFusion with Arrow columnar format
5. Results returned to JVM via Arrow FFI

## Key Patterns

### Spark Version Shims
The codebase uses a shim pattern to support multiple Spark versions:
- `spark-extension/` contains base classes with version-agnostic logic
- `spark-extension-shims-spark/` contains version-specific implementations
- `Shims` trait provides version-specific behavior injection

### Native Operators
Each native operator follows the pattern:
- JVM side: `NativeXxxExec` extends Spark's `SparkPlan` with `NativeSupports`
- Native side: `XxxExecNode` protobuf message → `xxx_exec.rs` implementation
- Operators in `datafusion-ext-plans/src/`: `parquet_exec.rs`, `agg_exec.rs`, `sort_exec.rs`, `shuffle_writer_exec.rs`, etc.

### JNI Bridge
- `JniBridge.java` (auron-core) declares native methods and callback utilities
- `jni_bridge.rs` (auron-jni-bridge) implements JNI bindings with macros like `jni_new_string!`, `jni_call!`
- Resource passing via `putResource`/`getResource` for objects like FileSystems, broadcast variables

## Project Structure Notes

- **Rust toolchain**: nightly (specified in `rust-toolchain.toml`)
- **Protobuf**: Physical plans serialized via protobuf; schema in `auron.proto`
- **Supported Spark versions**: 3.0, 3.1, 3.2, 3.3, 3.4, 3.5, 4.0, 4.1
- **Supported Scala versions**: 2.12 (default), 2.13 (for Spark 4.x)
- **Optional integrations**: Celeborn, Uniffle (remote shuffle), Paimon, Iceberg, Flink

## PR Guidelines

- PR title format: `[AURON #<issue-number>] Brief description`
- All changes require a GitHub issue first
- Major changes require an AIP (Auron Improvement Plan)
- Use squash and merge for PRs
