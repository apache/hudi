<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Metadata table services tool

`org.apache.hudi.utilities.HoodieMetadataTableServicesTool` is a standalone
Spark-submit entry point for MDT maintenance. It calls MDT writer APIs directly;
it does not require an HTTP Table Service Manager server.

## Delegation and compatibility

`hoodie.metadata.table.service.manager.actions` retains its execution-delegation
meaning and now additionally accepts `clean` and `archive`, alongside the existing
`compaction` and `logcompaction` values. Previously valid configurations keep their
behavior. Delegation remains disabled by default, so existing inline clean/archive
behavior does not change unless users explicitly opt in.

For example, to delegate only clean and archive, set these on the ingestion writer:

```properties
hoodie.metadata.table.service.manager.enabled=true
hoodie.metadata.table.service.manager.actions=clean,archive
```

This skips inline clean/archive; it does **not** submit an HTTP manager request or
automatically launch a replacement job. Users must separately deploy and schedule
the tool with `--services clean,archive --mode execute`, or provide another external
executor. Without one, the delegated maintenance will not run, potentially allowing
obsolete files and the active timeline to accumulate.

The tool follows its requested services and mode, bypassing ingestion-side
delegation checks so it does not delegate the work again. Scheduling delegation is
controlled separately by `hoodie.metadata.table.service.manager.schedule.actions`,
which supports only `compaction` and `logcompaction`.
