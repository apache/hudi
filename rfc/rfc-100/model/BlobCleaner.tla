---------------------------- MODULE BlobCleaner ----------------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more          *)
(* contributor license agreements.  See the NOTICE file distributed with      *)
(* this work for additional information regarding copyright ownership.        *)
(* The ASF licenses this file to You under the Apache License, Version 2.0    *)
(* (the "License"); you may not use this file except in compliance with       *)
(* the License.  You may obtain a copy of the License at                      *)
(*                                                                            *)
(*      http://www.apache.org/licenses/LICENSE-2.0                            *)
(*                                                                            *)
(* Unless required by applicable law or agreed to in writing, software        *)
(* distributed under the License is distributed on an "AS IS" BASIS,          *)
(* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   *)
(* See the License for the specific language governing permissions and        *)
(* limitations under the License.                                             *)
(* Trimmed RFC-100 blob cleaner protocol: writers with preCommit veto, cleaner  *)
(* with unlocked snapshot T0 + locked delta re-check + sidecar, rollback of    *)
(* failed commits, compaction-time GC of blob_refs. Safety only.               *)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS Writers, MaxAtt, MaxCyc, Paths, RefSets,
          VetoCompleted, GcRawFiles, NoVeto, NoLockedRecheck

VARIABLES commits, writers, lock, registry, filesMdt, phys, blobs, cleaner, done, owned
vars == <<commits, writers, lock, registry, filesMdt, phys, blobs, cleaner, done, owned>>

InitFile == "Finit"
FileOf(w, a) == "F" \o ToString(w) \o "_" \o ToString(a)
CleanIdle == [phase |-> "idle", T0 |-> {}, E |-> {}, cands |-> {}, sidecar |-> {}, cyc |-> 0]

Init ==
  /\ commits = << [file |-> InitFile, refs |-> Paths, state |-> "completed"] >>
  /\ writers = [w \in Writers |-> [phase |-> "idle", cid |-> 0, att |-> 0]]
  /\ lock = -1
  /\ registry = { <<p, InitFile, 1>> : p \in Paths }
  /\ filesMdt = { <<InitFile, 1>> }
  /\ phys = { InitFile }
  /\ blobs = Paths
  /\ cleaner = CleanIdle
  /\ done = {}
  /\ owned = Paths

Cids == 1..Len(commits)
Completed == { i \in Cids : commits[i].state = "completed" }

PendingSidecars ==
  (IF cleaner.phase \in {"requested", "slices_deleted", "blobs_deleted"} THEN cleaner.sidecar ELSE {})
  \cup (IF VetoCompleted THEN UNION done ELSE {})

SetCommit(i, st) == [commits EXCEPT ![i].state = st]
SetWriter(w, ph, c, a) == [writers EXCEPT ![w] = [phase |-> ph, cid |-> c, att |-> a]]

(* ---------------- writers ---------------- *)
WStart(w) ==
  /\ writers[w].phase = "idle" /\ writers[w].att < MaxAtt
  /\ \E refs \in RefSets :
       LET f == FileOf(w, writers[w].att) IN
       /\ commits' = Append(commits, [file |-> f, refs |-> refs, state |-> "started"])
       /\ writers' = SetWriter(w, "started", Len(commits) + 1, writers[w].att + 1)
       /\ phys' = phys \cup {f}
       /\ blobs' = blobs \cup refs
       /\ UNCHANGED <<lock, registry, filesMdt, cleaner, done, owned>>

WLock(w) ==
  /\ writers[w].phase = "started" /\ lock = -1
  /\ lock' = w
  /\ writers' = SetWriter(w, "locked", writers[w].cid, writers[w].att)
  /\ UNCHANGED <<commits, registry, filesMdt, phys, blobs, cleaner, done, owned>>

WPreCommit(w) ==
  /\ writers[w].phase = "locked"
  /\ LET c == commits[writers[w].cid] IN
     IF ~NoVeto /\ c.refs \cap PendingSidecars # {} THEN
        /\ commits' = SetCommit(writers[w].cid, "aborted")
        /\ writers' = SetWriter(w, "idle", 0, writers[w].att)
        /\ lock' = -1
        /\ phys' = phys \ {c.file}
        /\ UNCHANGED <<registry, filesMdt, blobs, cleaner, done, owned>>
     ELSE
        /\ writers' = SetWriter(w, "pre_ok", writers[w].cid, writers[w].att)
        /\ UNCHANGED <<commits, lock, registry, filesMdt, phys, blobs, cleaner, done, owned>>

WMdt(w) ==
  /\ writers[w].phase = "pre_ok"
  /\ LET cid == writers[w].cid
         c == commits[cid] IN
     /\ registry' = registry \cup { <<p, c.file, cid>> : p \in c.refs }
     /\ filesMdt' = filesMdt \cup { <<c.file, cid>> }
     /\ commits' = SetCommit(cid, "mdt")
     /\ writers' = SetWriter(w, "mdt", cid, writers[w].att)
     /\ UNCHANGED <<lock, phys, blobs, cleaner, done, owned>>

WCrashBeforeMdt(w) ==
  /\ writers[w].phase = "pre_ok"
  /\ commits' = SetCommit(writers[w].cid, "failed")
  /\ writers' = SetWriter(w, "idle", 0, writers[w].att)
  /\ lock' = -1
  /\ UNCHANGED <<registry, filesMdt, phys, blobs, cleaner, done, owned>>

WComplete(w) ==
  /\ writers[w].phase = "mdt"
  /\ commits' = SetCommit(writers[w].cid, "completed")
  /\ writers' = SetWriter(w, "idle", 0, writers[w].att)
  /\ lock' = -1
  /\ owned' = owned \cup commits[writers[w].cid].refs
  /\ UNCHANGED <<registry, filesMdt, phys, blobs, cleaner, done>>

WCrashAfterMdt(w) ==
  /\ writers[w].phase = "mdt"
  /\ commits' = SetCommit(writers[w].cid, "failed")
  /\ writers' = SetWriter(w, "idle", 0, writers[w].att)
  /\ lock' = -1
  /\ UNCHANGED <<registry, filesMdt, phys, blobs, cleaner, done, owned>>

Rollback(i) ==
  /\ lock = -1 /\ commits[i].state = "failed"
  /\ commits' = SetCommit(i, "rolled_back")
  /\ registry' = { e \in registry : e[3] # i }
  /\ filesMdt' = { e \in filesMdt : e[2] # i }
  /\ phys' = phys \ {commits[i].file}
  /\ UNCHANGED <<writers, lock, blobs, cleaner, done, owned>>

(* ---------------- cleaner ---------------- *)
CleanSnapshot ==
  /\ cleaner.phase = "idle" /\ cleaner.cyc < MaxCyc
  /\ LET T0 == Completed
         visible == { e \in filesMdt : e[2] \in T0 }
         vfiles == { e[1] : e \in visible }
     IN \E E \in SUBSET vfiles :
          /\ E # {}
          /\ LET refsInE == UNION { commits[i].refs : i \in { j \in Cids : commits[j].file \in E } }
                 Live(p) == \E e \in registry :
                              e[1] = p /\ e[3] \in T0 /\ <<e[2], e[3]>> \in visible /\ e[2] \notin E
                 cands == { p \in refsInE : ~Live(p) }
             IN cleaner' = [phase |-> "snap", T0 |-> T0, E |-> E, cands |-> cands,
                            sidecar |-> {}, cyc |-> cleaner.cyc]
  /\ UNCHANGED <<commits, writers, lock, registry, filesMdt, phys, blobs, done, owned>>

CleanPlanLocked ==
  /\ cleaner.phase = "snap" /\ lock = -1
  /\ LET new == Completed \ cleaner.T0
         surv == IF NoLockedRecheck THEN cleaner.cands
                 ELSE { p \in cleaner.cands : ~\E e \in registry : e[1] = p /\ e[3] \in new }
     IN cleaner' = [cleaner EXCEPT !.phase = "requested", !.sidecar = surv]
  /\ UNCHANGED <<commits, writers, lock, registry, filesMdt, phys, blobs, done, owned>>

CleanDeleteSlices ==
  /\ cleaner.phase = "requested"
  /\ phys' = phys \ cleaner.E
  /\ cleaner' = [cleaner EXCEPT !.phase = "slices_deleted"]
  /\ UNCHANGED <<commits, writers, lock, registry, filesMdt, blobs, done, owned>>

CleanDeleteBlobs ==
  /\ cleaner.phase = "slices_deleted"
  /\ blobs' = blobs \ cleaner.sidecar
  /\ owned' = owned \ cleaner.sidecar
  /\ cleaner' = [cleaner EXCEPT !.phase = "blobs_deleted"]
  /\ UNCHANGED <<commits, writers, lock, registry, filesMdt, phys, done>>

CleanComplete ==
  /\ cleaner.phase = "blobs_deleted" /\ lock = -1
  /\ filesMdt' = { e \in filesMdt : e[1] \notin cleaner.E }
  /\ done' = done \cup { cleaner.sidecar }
  /\ cleaner' = [CleanIdle EXCEPT !.cyc = cleaner.cyc + 1]
  /\ UNCHANGED <<commits, writers, lock, registry, phys, blobs, owned>>

(* ---------------- compaction-time GC ---------------- *)
GC ==
  LET present == IF GcRawFiles THEN { e[1] : e \in filesMdt }
                 ELSE { e[1] : e \in { g \in filesMdt : g[2] \in Completed } }
  IN \E e \in registry :
       /\ e[2] \notin present
       /\ registry' = registry \ {e}
       /\ UNCHANGED <<commits, writers, lock, filesMdt, phys, blobs, cleaner, done, owned>>

Next ==
  \/ \E w \in Writers : WStart(w) \/ WLock(w) \/ WPreCommit(w) \/ WMdt(w)
                        \/ WCrashBeforeMdt(w) \/ WComplete(w) \/ WCrashAfterMdt(w)
  \/ \E i \in Cids : Rollback(i)
  \/ CleanSnapshot \/ CleanPlanLocked \/ CleanDeleteSlices \/ CleanDeleteBlobs \/ CleanComplete
  \/ GC

Spec == Init /\ [][Next]_vars

(* ---------------- properties ---------------- *)
\* R1: a deleted blob is never referenced by a completed, physically present file.
R1 == \A p \in Paths : p \notin blobs =>
        \A i \in Cids : ~(commits[i].state = "completed" /\ commits[i].file \in phys /\ p \in commits[i].refs)

Quiescent == (\A w \in Writers : writers[w].phase = "idle") /\ cleaner.phase = "idle"

\* R2: when nothing is in flight, every owned blob is still referenced by a present file.
R2 == Quiescent =>
        \A p \in owned : p \in blobs =>
          \E i \in Cids : commits[i].state \in {"started", "mdt", "completed"}
                          /\ commits[i].file \in phys /\ p \in commits[i].refs
=============================================================================
