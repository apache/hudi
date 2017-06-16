# Change Log
All notable changes to this project will be documented in this file.

## [Unreleased]
### Added
No Items

### Changed
No Items

## [0.3.8] - 2017-06-15
### Checkpoints
- Merge on Read tested end to end. Ingestion - Hive Registration - Querying non-nested fields

### New Features
- [#149](https://github.com/uber/hoodie/issues/149)  Introduce custom log format (HoodieLogFormat) for the log files
- [#141](https://github.com/uber/hoodie/issues/141) Introduce Compaction Strategies for Merge on Read table and implement UnboundedCompactionStrategy and IOBoundedCompactionStrategy
- [#42](https://github.com/uber/hoodie/issues/42) Implement HoodieRealtimeInputFormat and HoodieRealtimeRecordReader
- [#150](https://github.com/uber/hoodie/issues/150) Rewrite hoodie-hive to incrementally sync partitions based on the last commit that was sucessfully synced

### Changes
- [Updated community committership guidelines](https://github.com/uber/hoodie/commit/1b0a0279428fc406d44db2fda6d6bf705cd1eb10)
- [Add GCS support](https://github.com/uber/hoodie/commit/43a55b09fdc64368f5bbaf17980233b84cf33760)
- [Add S3 support](https://github.com/uber/hoodie/commit/d6f94b998dc3b98308b86c4a6cd11e2a250a8913)
- [Support for viewFS](https://github.com/uber/hoodie/commit/3c984447da754e85bafeac81a790efdc5fee42dc)

Commits: [21e334...4b26be](https://github.com/uber/hoodie/compare/21e334592f30ef055097439ad5ca1fdf7debb78d...4b26be9f6178dde6cace2ed87ae3d2ae8b4ac827)
