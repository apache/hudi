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

# Streaming sink failures after a data commit

A Hudi write can publish its data commit and then fail during cleaning, archival,
or metastore synchronization. Retrying the entire write would create another
commit for the same Spark batch and can duplicate rows in insert/append tables.

The streaming sink checks durable batch metadata after a failed attempt. If the
batch is already committed, it fails the query without retrying the data write.
The original failure remains in the exception chain. If commit status cannot be
read, it also stops rather than risking another write. Failures before a commit
retain the configured retry behavior.

Resolve the reported post-commit problem, including separately rerunning a failed
metastore synchronization when needed, and restart with the same checkpoint and
streaming checkpoint identifier. The committed batch is skipped; this does not
rerun the failed post-commit operation. Do not delete the checkpoint to recover.

The existing explicit `hoodie.datasource.write.streaming.ignore.failed.batch=true`
opt-out retains its behavior. For reliable failure reporting keep it false.
This change does not add multi-writer/fan-in support or checkpoint-generation resets.

The regression suite injects a failure through a real metadata-sync tool after the
writer commits, for new and pre-existing COW/MOR tables. It checks one write attempt,
visible query failure, exact rows, an absent Spark commit log before restart, and
unchanged Hudi commits after restart from that checkpoint.
