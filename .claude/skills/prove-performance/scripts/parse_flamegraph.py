#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Parse an async-profiler 3.0 HTML flamegraph WITHOUT a browser.

async-profiler 3.0 packs the frames into a JS `const cpool=[...]` constant pool
(prefix-compressed strings) plus f()/u()/n() tree-builder calls. This reconstructs
the tree, then either:
  (default)            self-time by category (S3 list/read/write, GC, parquet, etc.) + top self frames
  --callers <substr>   the caller stacks of every frame whose name contains <substr>

Use the category report to see where wall/CPU goes; use --callers to attribute a hot
leaf (e.g. getAllLogFiles, listStatus, BitSet.ensureCapacity) to its call paths.

Examples:
  parse_flamegraph.py cpu-exec-11.html
  parse_flamegraph.py cpu-exec-11.html --callers getAllLogFiles
  parse_flamegraph.py wall-driver.html --callers listStatus
"""
import re, sys, bisect, collections, argparse

def parse(path):
    lines = open(path, encoding="utf-8", errors="replace").read().split("\n")
    start = next(i for i, l in enumerate(lines) if l.strip().startswith("const cpool"))
    cpool = []; i = start + 1
    while not lines[i].strip().startswith("];"):
        m = re.match(r"^'(.*)',?$", lines[i]); s = m.group(1) if m else ""
        cpool.append(s.replace("\\\\", "\\").replace("\\'", "'")); i += 1
    for k in range(1, len(cpool)):                       # undo prefix compression
        cur = cpool[k]; plen = ord(cur[0]) - 32 if cur else 0
        cpool[k] = cpool[k-1][:plen] + cur[1:]
    frames = []; level0 = left0 = width0 = 0             # (level,left,width,name)
    call = re.compile(r"^([fun])\(([0-9,]*)\)\s*$")
    def emit(key, level, left, width):
        nonlocal level0, left0, width0
        level0 = level; left0 += left; width0 = width if width else width0
        frames.append((level0, left0, width0, cpool[key>>3] if key>>3 < len(cpool) else "?"))
    for l in lines:
        m = call.match(l)
        if not m: continue
        op = m.group(1); a = [int(x) if x else None for x in m.group(2).split(",")]; key = a[0] or 0
        if op == "f":   emit(key, a[1] or 0, a[2] or 0, a[3] if len(a) > 3 else None)
        elif op == "u": emit(key, level0 + 1, 0, a[1] if len(a) > 1 else None)
        elif op == "n": emit(key, level0, width0, a[1] if len(a) > 1 else None)
    return frames

def index_by_level(frames):
    bylevel = {}
    for lv, lf, w, n in frames:
        bylevel.setdefault(lv, []).append((lf, w, n))
    for lv in bylevel:
        bylevel[lv].sort()
    return bylevel

def self_times(frames, bylevel):
    total = max((w for lv, lf, w, n in frames if lv == 0), default=0)
    self_by = {}
    for lv, lf, w, n in frames:
        kids = bylevel.get(lv + 1, [])
        ls = [k[0] for k in kids]; j = bisect.bisect_left(ls, lf); covered = 0
        while j < len(kids) and kids[j][0] < lf + w:
            covered += kids[j][1]; j += 1
        self_by[n] = self_by.get(n, 0) + (w - covered)
    return total, self_by

CATS = [
 ("S3 LIST/status", r"listStatus|[Ll]istObject|listLocatedStatus|getFileStatus|globStatus"),
 ("S3 read(GET)",   r"getObject|S3AInputStream|reopen|read.*[Ss]3|InputStream\.read"),
 ("S3 write(PUT)",  r"putObject|S3ABlockOutputStream|uploadPart|multipart|writeObject"),
 ("net/socket(blocked)", r"socketRead|SocketInputStream|socketRead0|NioSocketImpl|epoll|poll0|SocketChannel|read0"),
 ("TLS/SSL",        r"[Ss][Ss][Ll]|sun\.security|cipher|Cipher"),
 ("HTTP client",    r"[Hh]ttp|apache\.http|amazonaws"),
 ("marker",         r"[Mm]arker"),
 ("compression",    r"[Dd]eflat|[Zz]std|[Ss]nappy|[Ll]z4|[Gg][Zz]ip|GZIP|compress|Compressor"),
 ("parquet write/encode", r"[Pp]arquet.*[Ww]rite|ColumnWriter|writePage|RunLength|PlainValuesWriter|Dictionary.*Writer|encode"),
 ("parquet read/decode",  r"[Pp]arquet.*[Rr]ead|ColumnReader|readPage|decode|Decompressor"),
 ("log scan/merge", r"LogRecordScanner|HoodieMergedLog|scanInternal|HoodieLogFileReader|processQueued|AbstractHoodieLogRecord|ExternalSpillableMap"),
 ("bloom filter",   r"[Bb]loom|BitSet|ensureCapacity|util\.bloom"),
 ("avro",           r"[Aa]vro|GenericData|GenericDatum|SpecificData"),
 ("rocksdb/spill",  r"rocksdb|RocksDB|DiskBasedMap|BitCask|spill"),
 ("GC",             r"\bGC\b|G1|Garbage|[Cc]ollect|markWord|TLAB"),
 ("JIT/compiler",   r"Compile|C2Compiler|C1_|Interpreter|StubRoutines|nmethod"),
 ("kafka/source",   r"[Kk]afka|[Cc]onsumer|poll\b"),
 ("hashing/serde",  r"[Hh]ash|serializ|deserializ|kryo|Kryo"),
]

def report(path):
    frames = parse(path); bylevel = index_by_level(frames)
    total, self_by = self_times(frames, bylevel)
    print("=" * 78); print(f"  file: {path.split('/')[-1]}\n  total samples (root width): {total:,}")
    pats = [(name, re.compile(rx)) for name, rx in CATS]
    cat_self = {name: 0 for name, _ in CATS}; other = 0
    for n, s in self_by.items():
        hit = next((name for name, rx in pats if rx.search(n)), None)
        if hit: cat_self[hit] += s
        else: other += s
    print("  -- self-time by category (% of total) --")
    for v, k in sorted([(v, k) for k, v in cat_self.items() if v > 0], reverse=True):
        print(f"      {100*v/total:6.2f}%  {v:>8,}  {k}")
    print(f"      {100*other/total:6.2f}%  {other:>8,}  (uncategorized)")
    print("  -- top 20 self frames --")
    for n, s in sorted(self_by.items(), key=lambda kv: -kv[1])[:20]:
        if s <= 0: continue
        print(f"      {100*s/total:6.2f}%  {s:>8,}  {n[:90]}")

def callers(path, target):
    frames = parse(path); bylevel = index_by_level(frames)
    total = max((w for lv, lf, w, n in frames if lv == 0), default=1)
    def parent(lv, lf):
        if lv == 0: return None
        arr = bylevel.get(lv - 1, []); ls = [a[0] for a in arr]
        j = bisect.bisect_right(ls, lf) - 1
        if 0 <= j < len(arr) and arr[j][0] <= lf < arr[j][0] + arr[j][1]: return arr[j]
        return None
    matches = [(lv, lf, w, n) for lv, lf, w, n in frames if target in n]
    sw = sum(m[2] for m in matches)
    print(f"# total samples={total:,}  frames matching '{target}': {len(matches)}  sum width={sw:,} ({100*sw/total:.2f}%)")
    imm = collections.Counter(); paths = collections.Counter()
    for lv, lf, w, n in matches:
        p = parent(lv, lf); imm[p[2] if p else "(root)"] += w
        chain = []; clv, clf = lv, lf
        while True:
            pr = parent(clv, clf)
            if not pr: break
            chain.append(pr[2]); clv -= 1
            arr = bylevel.get(clv, []); ls = [a[0] for a in arr]; j = bisect.bisect_right(ls, clf) - 1
            clf = arr[j][0]
            if clv == 0 or len(chain) >= 8: break
        paths[" <- ".join(chain[:8])] += w
    print("\n## immediate callers of target (by samples):")
    for name, c in imm.most_common(8): print(f"   {100*c/total:5.2f}%  {c:>7,}  {name[:95]}")
    print("\n## top caller stacks (target <- parent <- ... up to 8):")
    for pth, c in paths.most_common(6): print(f"   {100*c/total:5.2f}%  {c:>7,}\n      {pth[:300]}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("flamegraph", help="async-profiler 3.0 .html")
    ap.add_argument("--callers", help="show caller stacks of frames containing this substring")
    a = ap.parse_args()
    callers(a.flamegraph, a.callers) if a.callers else report(a.flamegraph)
