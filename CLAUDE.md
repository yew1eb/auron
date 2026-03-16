# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Auron (Incubating) is a native vectorized execution accelerator for big data engines (primarily Apache Spark). It intercepts Spark's physical query plan, serializes it via Protocol Buffers, and executes it natively in Rust using Apache DataFusion and Apache Arrow for columnar/SIMD-accelerated computation.

## Technology Stack

- **Rust** (nightly, see `rust-toolchain.toml`): Native execution engine
- **Java/Scala**: Spark integration layer (JDK 8/11/17/21; Scala 2.12 default, 2.13 for Spark 4.x)
- **Protocol Buffers**: Query plan serialization between JVM and native layer
- **Apache DataFusion 49.0.0** (forked): Execution engine backend
- **Apache Arrow 55.2.0** (forked): Columnar data format

## Build Commands

```bash
# Quick local build (pre-release, Spark 3.5, Scala 2.12)
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12

# Skip native Rust build (Java/Scala-only changes)
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 -DskipBuildNative

# Direct Maven build
./build/mvn clean package -Pspark-3.5 -Pscala-2.12 -Ppre -DskipTests

# Run all Java/Scala tests
./build/mvn test -Pspark-3.5 -Pscala-2.12

# Run all Rust tests
cargo test --workspace --all-features
```

Supported Spark versions: 3.0–3.5, 4.0, 4.1. Output JAR lands in `target/` (local) or `target-docker/` (Docker builds).

## Code Formatting & Linting

```bash
# Format all code (run before committing)
./dev/reformat

# Check formatting without applying changes
./dev/reformat --check
```

- **Java**: Spotless + Palantir Java Format; Checkstyle (`dev/checkstyle.xml`)
- **Scala**: Scalafmt (`scalafmt.conf`); Scalastyle (`dev/scalastyle-config.xml`); max column 98; import order: `java.*`, `scala.*`, third-party, `org.apache.auron.*`
- **Rust**: rustfmt (`rustfmt.toml`); Clippy; `unwrap_used` and `panic` are denied
- **License**: All source files must include the Apache 2.0 header (`dev/license-header`). The build fails without it.
- **JUnit 4 is banned** — use JUnit 5 (Jupiter) for Java tests.

## Architecture

### Execution Flow

1. Spark optimizes its physical plan normally.
2. `AuronSparkSessionExtension` (registered via `spark.sql.extensions`) injects a `ColumnarRule`.
3. `AuronConvertStrategy` traverses the plan tree and tags each node as convertible or not.
4. `AuronConverters.convertSparkPlanRecursively` replaces Spark plan nodes with `Native*Exec` counterparts (e.g., `NativeFilterExec`, `NativeAggExec`, `NativeShuffleExchangeExec`).
5. Each `Native*Exec` implements `NativeSupports` and returns a `NativeRDD`.
6. At execution time, the plan is serialized to protobuf (`native-engine/auron-planner/proto/auron.proto`) and passed via JNI (`JniBridge.callNative`) to the Rust engine.
7. The Rust engine deserializes the plan, maps it to DataFusion physical nodes, and streams Arrow record batches back to the JVM.

### Key Layers

| Layer | Location | Purpose |
|-------|----------|---------|
| Spark extension entry | `spark-extension/` | `AuronSparkSessionExtension`, plan conversion, NativeRDD, shuffle |
| Spark version shims | `spark-extension-shims-spark/` | Version-specific `Native*Exec` implementations and `ShimsImpl` |
| Core JNI bridge (Java) | `auron-core/src/main/java/org/apache/auron/jni/` | `JniBridge` native method declarations, resource management |
| Native engine | `native-engine/auron/` | Main Rust CDYLIB entry point |
| JNI bridge (Rust) | `native-engine/auron-jni-bridge/` | Rust side of JNI, `JavaClasses` registry |
| Planner + protobuf | `native-engine/auron-planner/` | Protobuf definitions and plan deserialization |
| DataFusion extensions | `native-engine/datafusion-ext-plans/` | Custom physical plans (agg, shuffle, scan, joins, window, generate) |
| Expressions & functions | `native-engine/datafusion-ext-exprs/`, `datafusion-ext-functions/` | Custom expressions and UDFs |
| Memory management | `native-engine/auron-memmgr/`, `auron-core/.../memory/` | Multi-level spill management |
| Arrow I/O | `spark-extension/.../arrowio/` | Arrow FFI export, Arrow↔Spark type conversion |
| Common utilities | `native-engine/datafusion-ext-commons/` | Shared Rust utilities |

### Plan Conversion Pattern

Spark plan nodes are converted bottom-up. Each native plan node has two parts:
- A `Base` trait/class in `spark-extension/` (version-agnostic logic)
- A concrete `Exec` class in `spark-extension-shims-spark/` (Spark-version-specific implementation)

`NativeConverters.scala` handles expression translation from Spark's catalyst to protobuf `PhysicalExprNode`.

### Shuffle

- `AuronShuffleManager` replaces Spark's default shuffle manager.
- Optional integrations: Uniffle (`auron-uniffle/`), Celeborn (`auron-celeborn-0.5/`, `auron-celeborn-0.6/`).
- Native shuffle writes Arrow columnar batches in a compact binary format.

### Protocol Buffers

Plan serialization schema: `native-engine/auron-planner/proto/auron.proto`.
Generated Java classes: `dev/mvn-build-helper/proto/` (package `org.apache.auron.protobuf`).

## Pull Request Guidelines

- Title format: `[AURON #<issue-number>] Brief description`
- All PRs are squash-merged; create a GitHub issue before submitting
- Major changes require an Auron Improvement Plan (AIP)
