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

# TLA+ model of the trimmed RFC-100 blob cleaner

Same protocol as blob_cleaner_model.py (python3 blob_cleaner_model.py [--gc-visible |
--no-veto-completed | --no-veto | --no-locked-recheck]), transcribed to TLA+ for TLC.

Run (tla2tools.jar from https://github.com/tlaplus/tlaplus/releases):

    java -cp tla2tools.jar tlc2.TLC -deadlock -workers auto -config Base.cfg BlobCleaner.tla

Configs:
- Base.cfg: the proposal as written (veto covers REQUESTED/INFLIGHT/COMPLETED cleans,
  GC reads raw FILES). Expected: no violation.
- NoVetoCompleted.cfg: veto ignores COMPLETED cleans. Expected: R1 violation, writer that
  wrote its file before the clean commits after the clean completed.
- GcVisible.cfg: GC reads FILES through the completed-instant filter. Expected: R1 violation,
  entry of an inflight commit dropped, commit then completes with no registry entry.
- NoVeto.cfg: no writer veto. Expected: R1 violation, timing C.
- NoLockedRecheck.cfg: no locked delta re-check. Expected: R1 violation, timing B.

Invariants: R1 (a deleted blob is never referenced by a completed, physically present file),
R2 (when nothing is in flight, every owned blob is referenced by a present file).
Safety only; no fairness, so run with -deadlock.
