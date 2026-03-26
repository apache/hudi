# Internal Uber Components (Optional)

This package contains integration with internal Uber services and is **optional** for Apache Hudi users.

## Components

- **AthenaIngestionGateway** - Integration with Uber's Athena Ingestion Gateway service
- **FlinkHudiMuttleyClient** - RPC client using Uber's Muttley framework

## Usage

These components are used by `FlinkCheckpointClient` for Kafka offset checkpoint validation in Uber's infrastructure.

**For open-source Apache Hudi users**: You can safely ignore this package. The checkpoint validation feature requires access to Uber's internal services and will not function outside of Uber's infrastructure.

## Dependencies

- Requires Uber's internal Muttley RPC framework
- Requires access to Athena Ingestion Gateway service
