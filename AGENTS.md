<!--
- Licensed to the Apache Software Foundation (ASF) under one or more
- contributor license agreements.  See the NOTICE file distributed with
- this work for additional information regarding copyright ownership.
- The ASF licenses this file to You under the Apache License, Version 2.0
- (the "License"); you may not use this file except in compliance with
- the License.  You may obtain a copy of the License at
-
-   http://www.apache.org/licenses/LICENSE-2.0
-
- Unless required by applicable law or agreed to in writing, software
- distributed under the License is distributed on an "AS IS" BASIS,
- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
- See the License for the specific language governing permissions and
- limitations under the License.
-->

# Apache Auron (Incubating) - Agent Guide

This document provides essential information for AI coding agents working on the Apache Auron project.

## Project Overview

Apache Auron is an accelerator for big data engines (e.g., Apache Spark, Apache Flink) that leverages native vectorized execution to accelerate query processing. It combines the power of [Apache DataFusion](https://arrow.apache.org/datafusion/) and the scale of distributed computing frameworks.

**Key Capabilities:**
- **Native execution**: Implemented in Rust, eliminating JVM overhead
- **Vectorized computation**: Built on Apache Arrow's columnar format, leveraging SIMD instructions
- **Pluggable architecture**: Integrates with Apache Spark, designed for extensibility to other engines
- **Production optimizations**: Multi-level memory management, compacted shuffle formats, adaptive execution

## Technology Stack

### Languages and Runtimes
- **Rust**: Nightly toolchain (specified in `rust-toolchain.toml`)
- **Java/Scala**: JDK 8, 11, 17, or 21 (JDK 17+ required for Spark 4.x)
- **Scala**: 2.12 (default) or 2.13 (2.13 required for Spark 4.x)

### Core Dependencies
- **Apache DataFusion**: 49.0.0 (forked with custom patches)
- **Apache Arrow**: 55.2.0 (forked with custom patches)
- **Apache Spark**: 3.0.x to 4.1.x (multiple versions supported)
- **Apache Flink**: 1.18.x (optional)

### Build Tools
- **Maven**: 3.9.12 (bundled in `build/`)
- **Cargo**: Rust package manager

## Project Structure

```
/Users/admin/Workspaces/auron-spark3/
├── native-engine/           # Rust native execution engine
│   ├── auron/              # Main Rust library (CDYLIB)
│   ├── auron-jni-bridge/   # JNI bridge for Java interop
│   ├── auron-planner/      # Query planner with protobuf definitions
│   ├── auron-memmgr/       # Memory management
│   ├── datafusion-ext-commons/   # DataFusion common extensions
│   ├── datafusion-ext-exprs/     # Custom expression implementations
│   ├── datafusion-ext-functions/ # Custom function implementations
│   └── datafusion-ext-plans/     # Custom physical plan nodes
├── spark-extension/        # Spark integration layer
├── spark-extension-shims-spark/  # Spark version-specific shims
├── auron-core/            # Core Java/Scala components
├── common/                # Common utilities
├── auron-flink-extension/ # Flink integration (optional)
├── thirdparty/            # Third-party integrations
│   ├── auron-celeborn-0.5/
│   ├── auron-celeborn-0.6/
│   ├── auron-uniffle/
│   ├── auron-paimon/
│   └── auron-iceberg/
├── auron-spark-tests/     # Integration tests
├── auron-spark-ui/        # Spark UI extensions
├── hadoop-shim/           # Hadoop compatibility layer
├── dev/                   # Development tools
│   ├── docker-build/      # Docker build configurations
│   ├── mvn-build-helper/  # Maven build helpers
│   ├── reformat           # Code formatting script
│   ├── checkstyle.xml     # Checkstyle configuration
│   └── scalastyle-config.xml  # Scalastyle configuration
└── build/                 # Build tools (Maven wrapper)
```

## Build System

### Prerequisites
1. Install Rust nightly: `rustup show` (reads `rust-toolchain.toml`)
2. Install JDK (8, 11, 17, or 21) and set `JAVA_HOME`
3. Maven 3.9.11+ (or use bundled `./build/mvn`)

### Unified Build Script

The project provides `auron-build.sh` for consistent builds:

```bash
# Quick local build (pre-release profile)
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12

# Release build
./auron-build.sh --release --sparkver 3.5 --scalaver 2.12

# Skip native Rust build (for Java/Scala-only changes)
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 -DskipBuildNative

# Docker build
./auron-build.sh --docker true --image centos7 --release --sparkver 3.5 --scalaver 2.12

# Run tests (disabled by default)
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --skiptests false
```

**Supported Versions:**
- Spark: 3.0, 3.1, 3.2, 3.3, 3.4, 3.5, 4.0, 4.1
- Scala: 2.12, 2.13
- Docker images: centos7, ubuntu24, rockylinux8, debian11, azurelinux3

### Maven Profiles

Key Maven profiles for different configurations:
- **Spark versions**: `spark-3.0`, `spark-3.1`, ..., `spark-4.1`
- **Scala versions**: `scala-2.12`, `scala-2.13`
- **Build modes**: `pre` (fast), `release` (optimized)
- **Optional integrations**: `celeborn-0.5`, `celeborn-0.6`, `uniffle-0.10`, `paimon-1.2`, `flink-1.18`, `iceberg-1.9`

### Direct Maven Usage

```bash
# Build with specific Spark and Scala versions
./build/mvn clean package -Pspark-3.5 -Pscala-2.12 -Ppre -DskipTests

# Build with tests
./build/mvn clean package -Pspark-3.5 -Pscala-2.12 -Ppre

# Skip native Rust build
./build/mvn clean package -Pspark-3.5 -Pscala-2.12 -Ppre -DskipBuildNative
```

## Code Style Guidelines

### Automatic Formatting

Use the provided reformat script before committing:

```bash
# Format all code
./dev/reformat

# Check formatting without changes
./dev/reformat --check
```

### Java Code Style
- **Formatter**: Spotless with Palantir Java Format
- **Linter**: Checkstyle (configuration: `dev/checkstyle.xml`)
- **License header**: Apache 2.0 (file: `dev/license-header`)

### Scala Code Style
- **Formatter**: Scalafmt (configuration: `scalafmt.conf`)
- **Linter**: Scalastyle (configuration: `dev/scalastyle-config.xml`)
- **Import order**: java.*, scala.*, third-party, org.apache.auron.*
- **Max column**: 98

### Rust Code Style
- **Formatter**: rustfmt (configuration: `rustfmt.toml`)
- **Linter**: Clippy (configuration in `Cargo.toml` workspace lints)
- **Key lint rules**:
  - `unwrap_used = "deny"`
  - `panic = "deny"`

## Testing

### Rust Tests

```bash
# Run all Rust tests
cargo test --workspace --all-features

# Run with release optimizations
cargo test --workspace --all-features --release
```

### Scala/Java Tests

```bash
# Run all tests via build script
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --skiptests false

# Run tests via Maven
./build/mvn test -Pspark-3.5 -Pscala-2.12

# Run Spark-specific tests
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --sparktests true
```

### Test Frameworks
- **Scala**: ScalaTest 3.2.9
- **Java**: JUnit 5 (Jupiter)

### CI/CD

GitHub Actions workflows in `.github/workflows/`:
- **tpcds.yml**: TPC-DS benchmark tests across Spark versions
- **rust-test.yml**: Rust linting (fmt, clippy) and tests
- **style.yml**: Code style checks
- **license.yml**: License header verification
- **build-amd64-releases.yml / build-arm-releases.yml**: Release builds

## Key Configuration Files

| File | Purpose |
|------|---------|
| `Cargo.toml` | Rust workspace configuration |
| `Cargo.lock` | Rust dependency lock |
| `rust-toolchain.toml` | Rust nightly version specification |
| `rustfmt.toml` | Rust formatting rules |
| `pom.xml` | Maven parent POM |
| `scalafmt.conf` | Scala formatting rules |
| `scalafix.conf` | Scala linting rules |
| `dev/checkstyle.xml` | Java linting rules |
| `dev/scalastyle-config.xml` | Scala style rules |
| `dev/license-header` | License header template |

## JNI Bridge

The project uses JNI for Java-Rust interop:
- **Rust side**: `native-engine/auron-jni-bridge/`
- **Java side**: Classes in `auron-core` and `spark-extension`
- **Native library**: Built as `libauron.dylib` (macOS) or `libauron.so` (Linux)

## Protocol Buffers

Query plan serialization uses Protocol Buffers:
- **Definition**: `native-engine/auron-planner/proto/auron.proto`
- **Generated code**: Java classes in `dev/mvn-build-helper/proto/`

## Security Considerations

- All source files MUST include the Apache 2.0 license header
- The build fails if license headers are missing (Apache RAT plugin)
- JUnit 4 is banned; use JUnit 5 (Jupiter) for Java tests
- Restricted imports are enforced via Maven Enforcer plugin

## Development Tips

### IDE Setup
- **IntelliJ IDEA** with Scala plugin recommended
- **RustRover** or **VS Code with rust-analyzer** for Rust

### Common Build Issues
1. **Java version mismatch**: Ensure `JAVA_HOME` points to the correct JDK
2. **Rust toolchain**: Run `rustup show` to install the correct nightly
3. **protoc**: Required for building; install Protocol Buffers compiler

### Pull Request Guidelines
- Format: `[AURON #<issue-number>] Brief description`
- All PRs are squash-merged
- At least one committer approval required
- Create a GitHub issue before submitting PRs
- Major changes require an Auron Improvement Plan (AIP)

## Resources

- **Mailing list**: dev@auron.apache.org
- **Repository**: https://github.com/apache/auron
- **Documentation**: https://auron.apache.org/
- **Issue tracking**: GitHub Issues
