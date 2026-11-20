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

# HFile case study — the keyLength encoding bug (apache/hudi#18465)

The worked example behind the `author-changes` rules. Read this when working in `hudi-io`, hfile,
or storage-format code; the concrete facts here are also the reference for anyone touching
the native writer's on-disk bytes.

## The bug

Two different variable-length integer encodings live in HFile:

- The **block index per-entry keyLength** uses Hadoop `WritableUtils` VInt, decoded by
  `IOUtils.readVarLong`.
- The **trailer and file-info length prefixes** use Protobuf varint
  (`writeDelimitedTo` / `parseDelimitedFrom`).

They are byte-identical for values 0..127 and diverge at ≥ 128. An ambiguously named helper,
`getVariableLengthEncodedBytes`, silently used the wrong one and corrupted long keys.

**The fix**: encoding-specific names on the class whose framing each defines —
`HFileIndexBlock.getVarIntBytes` (Hadoop) and `HFileFileInfoBlock.getProtobufVarIntBytes`
(Protobuf) — each javadoc cross-referencing the other. `get*Bytes` (returns a `byte[]`)
reads truer than `write*`.

## Why the existing tests missed it

The bug needed four conditions simultaneously:

1. a **point lookup** (not a full scan),
2. of a **block-boundary key** (the ones stored in the index),
3. in a **multi-block file** (small block size + many records),
4. read by the **HBase reader** specifically.

Every passing test relaxed at least one. That is the shape of the false confidence to look
for: not "there were no tests", but "the tests avoided the intersection".

## The divergence threshold, derived

The index keyLength diverges at value ≥ 128, i.e. content length ≥ 116, because
`kvKeyLength = content + 12`. Short keys — the default in the existing tests — hide the bug
entirely. Test multiple lengths spanning 1-, 2-, and 3-byte vint widths (116, 200, 300) plus
the exact boundary, so no single length accidentally coincides on encoding bytes.

## Reachability: wrong bytes vs wrong reads

The native `hudi-io` reader is forward-only (`seekTo` / `next`; a backward `seekTo` throws)
and never reads the data block's previous-block-offset. The HBase reader does, in
`seekBefore()`.

So the writer setting each multi-block data block's `prevBlockOffset` to *its own* offset
(it should be the previous block's) is **latent** for the Hudi reader and a **real
correctness bug** for an HBase reader doing reverse positioning. Grep the reader for the
offset constant and check for a backward-seek path before calling a wrong field broken or
harmless — then fix it anyway, because a compatible reader relies on it.

## Native-vs-HBase byte equality holds only for a single-block file

Across multiple blocks the HBase writer differs in two ways:

1. **Block boundaries** — it accumulates past `blockSize`, while the native writer flushes
   before a record would exceed it. They align only when every record exceeds the block
   size.
2. **Index entries** — it stores shortened "fake first key" separators rather than full
   keys, even with a shared key prefix. This is the same optimization that trips the 0.x
   HFile reader.

Consequences for tests: pin the native-vs-HBase golden on **one block** (one full-key index
entry). Cover the multi-length vint widths with native-only goldens plus encoder unit tests.
The multi-block *data region* does match HBase once `prevBlockOffset` is correct and records
exceed the block size; the *root index* never does.

## The test matrix that resulted

- Writer × reader cells: native-write/native-read, native-write/HBase-read,
  HBase-write/native-read, HBase-write/HBase-read — each labelled *broken* or merely
  *uncovered*, parametrized over `{NONE, GZIP}` where relevant.
- Byte-level block assertions field by field: keyLen, valLen, rowLen, row, cfLen, ts, type,
  mvcc. Build the block directly via `createDataBlockToWrite` / `add` and
  `createRootIndexBlockToWrite` / `add`, then `getUncompressedBlockDataToWrite()`.
- A NONE-compression, fixed-input golden over the data block plus root index block, to lock
  the format. Not a whole-file compare.
- Consolidation: `TestHFileCompatibility` and `TestHFileReadCompatibility` were
  near-duplicates and merged. The `prevBlockOffset` chain check folded into the existing
  50-record multi-block `testUniqueKeyLocation` rather than becoming a new method — but the
  HBase-`seekBefore` check stayed in the cross-reader suite, where its reader lives, not in
  `TestHFileWriter`.
- The +10-bytes-per-index-entry KeyValue suffix changed the golden's expected file size; the
  new number carries a comment explaining why.

## Format-spec facts worth carrying forward

The KeyValue key suffix written by the native writer: `cfLen=0`, `ts=LATEST`, `type=Put`.
The data block and the root index block must serialize the *same* KeyValue key bytes,
because a point lookup compares index keys against data keys — which is why one shared
writer, not two encoders, is the correct structure.
