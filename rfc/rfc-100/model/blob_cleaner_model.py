# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Explicit-state model of the trimmed RFC-100 blob cleaner protocol.

Processes: NW writers (each up to MAXATT commit attempts), one cleaner (up to MAXCYC cycles),
rollback of failed commits, compaction-time GC of blob_refs.
Checks R1 (no visible, physically present file references a deleted blob) and
R2 (every owned blob is referenced by a present file or sits in a pending sidecar).
Default run is the design as written. Ablation flags, each expected to print an R1
counterexample: --gc-visible (GC reads FILES through the completed-instant filter instead of
raw), --no-veto-completed (writer veto ignores COMPLETED cleans), --no-veto, --no-locked-recheck.
"""
import sys, itertools
from collections import deque

NW, MAXATT, MAXCYC = 2, 2, 2
PATHS = ("P",)
REFSETS = (frozenset({"P"}), frozenset())
GC_RAW_FILES = "--gc-visible" not in sys.argv
VETO_COMPLETED = "--no-veto-completed" not in sys.argv
NO_VETO = "--no-veto" in sys.argv
NO_LOCKED_RECHECK = "--no-locked-recheck" in sys.argv

# commit record: (file, refs, state)  state in started|mdt|completed|failed|rolled_back|aborted
# writer: (phase, cid, attempts)  phase in idle|started|locked|pre_ok|mdt
# cleaner: (phase, T0, E, cands, sidecar, cycles)  phase in idle|snap|requested|slices_deleted|blobs_deleted
# state: (commits, writers, lock, registry, files_mdt, phys, blobs, cleaner, done_sidecars, owned)
# owned: paths whose current bytes are referenced by some completed commit (Hudi is responsible for them)

def initial():
    commits = (("F_init", frozenset({"P"}), "completed"),)
    writers = tuple(("idle", -1, 0) for _ in range(NW))
    registry = frozenset({("P", "F_init", 0)})
    files_mdt = frozenset({("F_init", 0)})
    phys = frozenset({"F_init"})
    blobs = frozenset(PATHS)
    cleaner = ("idle", frozenset(), frozenset(), frozenset(), frozenset(), 0)
    return (commits, writers, None, registry, files_mdt, phys, blobs, cleaner, frozenset(), frozenset({"P"}))

def completed_set(commits):
    return frozenset(i for i, c in enumerate(commits) if c[2] == "completed")

def pending_sidecars(cleaner, done_sidecars):
    s = set()
    if cleaner[0] in ("requested", "slices_deleted", "blobs_deleted"):
        s |= cleaner[4]
    if VETO_COMPLETED:
        for sc in done_sidecars: s |= sc
    return s

def successors(st):
    commits, writers, lock, registry, files_mdt, phys, blobs, cleaner, done, owned = st
    # ---- writers
    for w, (phase, cid, att) in enumerate(writers):
        if phase == "idle" and att < MAXATT:
            for refs in REFSETS:
                f = f"F{w}_{att}"
                nc = commits + ((f, refs, "started"),)
                nw = writers[:w] + (("started", len(commits), att + 1),) + writers[w+1:]
                nb = blobs | refs  # writer uploads / re-creates the bytes it references
                yield f"w{w} start {f} refs={set(refs)}", (nc, nw, lock, registry, files_mdt, phys | {f}, nb, cleaner, done, owned)
        elif phase == "started" and lock is None:
            nw = writers[:w] + (("locked", cid, att),) + writers[w+1:]
            yield f"w{w} lock", (commits, nw, ("w", w), registry, files_mdt, phys, blobs, cleaner, done, owned)
        elif phase == "locked":
            f, refs, _ = commits[cid]
            if not NO_VETO and refs & pending_sidecars(cleaner, done):
                nc = commits[:cid] + ((f, refs, "aborted"),) + commits[cid+1:]
                nw = writers[:w] + (("idle", -1, att),) + writers[w+1:]
                yield f"w{w} preCommit VETO abort", (nc, nw, None, registry, files_mdt, phys - {f}, blobs, cleaner, done, owned)
            else:
                nw = writers[:w] + (("pre_ok", cid, att),) + writers[w+1:]
                yield f"w{w} preCommit ok", (commits, nw, lock, registry, files_mdt, phys, blobs, cleaner, done, owned)
        elif phase == "pre_ok":
            f, refs, _ = commits[cid]
            nreg = registry | {(p, f, cid) for p in refs}
            nfiles = files_mdt | {(f, cid)}
            nc = commits[:cid] + ((f, refs, "mdt"),) + commits[cid+1:]
            nw = writers[:w] + (("mdt", cid, att),) + writers[w+1:]
            yield f"w{w} MDT delta commit", (nc, nw, lock, nreg, nfiles, phys, blobs, cleaner, done, owned)
            nc2 = commits[:cid] + ((f, refs, "failed"),) + commits[cid+1:]
            nw2 = writers[:w] + (("idle", -1, att),) + writers[w+1:]
            yield f"w{w} crash before MDT", (nc2, nw2, None, registry, files_mdt, phys, blobs, cleaner, done, owned)
        elif phase == "mdt":
            f, refs, _ = commits[cid]
            nc = commits[:cid] + ((f, refs, "completed"),) + commits[cid+1:]
            nw = writers[:w] + (("idle", -1, att),) + writers[w+1:]
            yield f"w{w} saveAsComplete", (nc, nw, None, registry, files_mdt, phys, blobs, cleaner, done, owned | refs)
            nc2 = commits[:cid] + ((f, refs, "failed"),) + commits[cid+1:]
            yield f"w{w} crash after MDT", (nc2, nw, None, registry, files_mdt, phys, blobs, cleaner, done, owned)
    # ---- rollback of failed commits (takes and releases the lock atomically)
    if lock is None:
        for i, (f, refs, s) in enumerate(commits):
            if s == "failed":
                nc = commits[:i] + ((f, refs, "rolled_back"),) + commits[i+1:]
                nreg = frozenset(e for e in registry if e[2] != i)
                nfiles = frozenset(e for e in files_mdt if e[1] != i)
                yield f"rollback c{i} ({f})", (nc, writers, None, nreg, nfiles, phys - {f}, blobs, cleaner, done, owned)
    # ---- cleaner
    phase, T0, E, cands, sidecar, cyc = cleaner
    if phase == "idle" and cyc < MAXCYC:
        T0n = completed_set(commits)
        visible_files = {(f, c) for (f, c) in files_mdt if c in T0n}
        vf = sorted(f for f, _ in visible_files)
        for k in range(1, len(vf) + 1):
            for En in itertools.combinations(vf, k):
                En = frozenset(En)
                refs_in_E = set()
                for f in En:
                    for (ff, rr, ss) in commits:
                        if ff == f: refs_in_E |= rr
                cn = set()
                for p in refs_in_E:
                    live = any(c in T0n and (f, c) in visible_files and f not in En
                               for (pp, f, c) in registry if pp == p)
                    if not live: cn.add(p)
                ncl = ("snap", T0n, En, frozenset(cn), frozenset(), cyc)
                yield f"clean snapshot E={set(En)} cands={cn}", (commits, writers, lock, registry, files_mdt, phys, blobs, ncl, done, owned)
    elif phase == "snap" and lock is None:
        if NO_LOCKED_RECHECK:
            surv = cands
        else:
            new = completed_set(commits) - T0
            surv = frozenset(p for p in cands if not any(pp == p and c in new for (pp, f, c) in registry))
        ncl = ("requested", T0, E, cands, surv, cyc)
        yield f"clean plan locked re-check sidecar={set(surv)}", (commits, writers, None, registry, files_mdt, phys, blobs, ncl, done, owned)
    elif phase == "requested":
        ncl = ("slices_deleted", T0, E, cands, sidecar, cyc)
        yield "clean delete slices", (commits, writers, lock, registry, files_mdt, phys - E, blobs, ncl, done, owned)
    elif phase == "slices_deleted":
        ncl = ("blobs_deleted", T0, E, cands, sidecar, cyc)
        yield f"clean delete blobs {set(sidecar)}", (commits, writers, lock, registry, files_mdt, phys, blobs - sidecar, ncl, done, owned - sidecar)
    elif phase == "blobs_deleted" and lock is None:
        nfiles = frozenset(e for e in files_mdt if e[0] not in E)
        ncl = ("idle", frozenset(), frozenset(), frozenset(), frozenset(), cyc + 1)
        yield "clean complete (FILES deletes)", (commits, writers, None, registry, nfiles, phys, blobs, ncl, done | {sidecar}, owned)
    # ---- compaction-time GC of blob_refs
    if GC_RAW_FILES:
        present = {f for (f, c) in files_mdt}
    else:
        comp = completed_set(commits)
        present = {f for (f, c) in files_mdt if c in comp}
    for e in registry:
        if e[1] not in present:
            yield f"GC drop {e}", (commits, writers, lock, registry - {e}, files_mdt, phys, blobs, cleaner, done, owned)

def check(st):
    commits, writers, lock, registry, files_mdt, phys, blobs, cleaner, done, owned = st
    for p in PATHS:
        if p not in blobs:
            for (f, refs, s) in commits:
                if s == "completed" and f in phys and p in refs:
                    return f"R1: blob {p} deleted but completed file {f} is present and references it"
    for p in owned:
        if p in blobs:
            referenced = any(s in ("started", "mdt", "completed") and f in phys and p in refs for (f, refs, s) in commits)
            pending = cleaner[0] in ("requested", "slices_deleted") and p in cleaner[4]
            quiescent = all(w[0] == "idle" for w in writers) and cleaner[0] == "idle"
            if not referenced and not pending and quiescent:
                return f"R2: blob {p} is an orphan with nothing left to reclaim it"
    return None

def run():
    init = initial()
    seen = {init: None}
    q = deque([init])
    labels = {}
    n = 0
    while q:
        st = q.popleft(); n += 1
        err = check(st)
        if err:
            trace = []
            cur = st
            while seen[cur] is not None:
                prev, lab = seen[cur]
                trace.append(lab); cur = prev
            print("VIOLATION:", err)
            for i, lab in enumerate(reversed(trace), 1): print(f"  {i:2d}. {lab}")
            print(f"states explored: {n}")
            return 1
        for lab, nx in successors(st):
            if nx not in seen:
                seen[nx] = (st, lab); q.append(nx)
    print(f"OK: no violation. states={len(seen)}")
    return 0

if __name__ == "__main__":
    sys.exit(run())
