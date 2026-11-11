<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Structured-streaming source recovery: 1.3.0 migration note

Latest-state Hudi structured-streaming readers now default to **failing** when a
saved offset cannot be read safely because history was archived/cleaned or the
upstream table was reset/recreated. This affects plain `readStream` users as well
as Spark Declarative Pipelines. Previously incremental full-table-scan fallback
could silently scan after history loss.

The new `hoodie.datasource.read.streaming.recovery.strategy` takes precedence
over `hoodie.datasource.read.incr.fallback.fulltablescan.enable` on resumed incremental reads. Fresh earliest starts retain the configured
full-scan fallback for rebuilding from retained table state:

- `FAIL` (default): stop with an actionable error; coordinate rebuilding the
  downstream tables and checkpoints.
- `READ_UPTO_LATEST_COMMIT`: explicitly recover with one full snapshot batch,
  then resume increments. Append targets can duplicate rows and upsert targets
  can retain keys deleted upstream. It is not downstream reconciliation.
- `READ_LATEST`: explicitly skip the unread range and accept data loss.

Initial start-offset selection and CDC reads are unchanged. Restart queries to
change the strategy. SDP internal sources must pass explicit recovery through
`spark.hoodie.datasource.read.streaming.recovery.strategy` in flow SQLConf.
See [source recovery and operational limits](../hudi-integ-test/spark-pipelines/SOURCE_SAFETY.md).
