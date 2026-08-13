<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Hudi native text index

This dependency-free Rust crate prototypes executable contracts from
[RFC-110](../rfc/rfc-110/rfc-110.md):

- stable document addresses;
- segment-level corpus statistics;
- BM25 scoring; and
- a versioned, validated binary envelope for native index files.

It deliberately does not yet implement tokenization, an FST term dictionary,
posting-list construction, block-max WAND, JNI, or Maven integration. Those
pieces should only be added after the RFC's storage and query contracts have
been reviewed.

Run the checks with:

```shell
cargo test --manifest-path hudi-native-text-index/Cargo.toml
```
