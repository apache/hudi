# Internal Uber Components (Optional)

This package contains integration with internal Uber services and is **optional** for Apache Hudi users.

## Components

- **FlinkHudiMuttleyClient** - Abstract base class providing HTTP retry logic for Muttley RPC communication
- **AthenaIngestionGateway** - Concrete Muttley RPC client for Uber's Athena Ingestion Gateway service (extends `FlinkHudiMuttleyClient`)
- **FlinkHudiMuttleyException** - Base exception for Muttley errors
- **FlinkHudiMuttleyClientException** - Exception for client-side Muttley errors
- **FlinkHudiMuttleyServerException** - Exception for server-side Muttley errors

## Usage

These components are used by `FlinkCheckpointClient` to collect Kafka offset metadata and attach it to Hudi commits as part of Uber's Kafka offset tracking feature.

**Feature flag**: The feature is controlled by `write.extra.metadata.enabled` (default: `false`). It is disabled by default and has no effect in standard Apache Hudi deployments.

**For open-source Apache Hudi users**: You can safely ignore this package. If `write.extra.metadata.enabled` is inadvertently set to `true` outside of Uber's infrastructure, Kafka offset collection will be silently skipped and Hudi commits will proceed normally (fail-open behavior).

## Dependencies

- Runtime: Uber's internal Muttley RPC framework
- Runtime: Access to Athena Ingestion Gateway service
- Compile-time: Jackson (`jackson-databind`) for JSON serialization of RPC request/response payloads
