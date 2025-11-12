<!--
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
-->

# HFile Format

[HFile format](https://hbase.apache.org/book.html#_hfile_format_2) is based on SSTable file format optimized for range
scans/point lookups, originally designed and implemented by [HBase](https://hbase.apache.org/). We use HFile version 3
as the base file format of the internal metadata table (MDT). Here we describe the HFile format that are relevant to
Hudi, as not all features of HFile are used.

The HFile is structured as follows:

```
+----------+-----------------------+
| "Scanned | Data Block            |
| block"   +-----------------------+
| section  | ...                   |
|          +-----------------------+
|          | Data Block            |
+----------+-----------------------+
| "Non-    | Meta Block            |
| scanned  +-----------------------+
| block"   | ...                   |
| section  +-----------------------+
|          | Meta Block            |
+----------+-----------------------+
| "Load-   | Root Data Index Block |
| on-open" +-----------------------+
| section  | Meta Index Block      |
|          +-----------------------+
|          | File Info Block       |
+----------+-----------------------+
| Trailer  | Trailer, containing   |
|          | fields and            |
|          | HFile Version         |
+----------+-----------------------+
```

- **"Scanned block" section**: this section contains all the data in key-value pairs, organized into one or multiple
  data
  blocks. This section has to be scanned for reading a key-value pair;
- **"Non-scanned block" section**: this section contains meta information, such as bloom filter which is used by Hudi to
  store the bloom filter, organized into one or multiple meta blocks. This section can be skipped for reading all
  key-value pairs sequentially from the beginning of the file.
- **"Load-on-open" section**: this section contains block index and file info, organized into three blocks:
    - **Root Data Index Block**: Index of data blocks in "Scanned block" section, containing the start offset in the
      file, size of the block on storage, and the first key of the data block;
    - **Meta Index Block**: Index of meta blocks in "Non-scanned block" section, containing the start offset in the
      file, size of the block on storage, and the key of the meta block;
    - **File Info Block**: HFile information that is useful for scanning the key-value pairs;
- **Trailer**: this section contains the information of all other sections and HFile version for decoding and parsing.
  This section is always read first when reading a HFile.

Next, we describe the block format and each block in details.

## Block format

All the blocks except for Trailer share the same format as follows:

```
 0                   1                   2                   3   
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
+                         Block Magic                           +
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                 On-disk Size Without Header                   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|               Uncompressed Size Without Header                |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
+                    Previous Block Offset                      +
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Checksum Type |              Bytes Per Checksum               >
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
>               |         On-disk Data Size With Header         >
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
>               |                                               |
+-+-+-+-+-+-+-+-+                                               +
|                                                               |
~                             Data                              ~
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
~                           Checksum                            ~
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

Note that one tick mark represents one bit position.
```

Header:

- **Block Magic**: 8 bytes, a sequence of bytes indicating the block type. Supported block types are:
    - `DATABLK*`: `DATA` block type for data blocks
    - `METABLKc`: `META` block type for meta blocks
    - `IDXROOT2`: `ROOT_INDEX` block type for root-level index blocks
    - `FILEINF2`: `FILE_INFO` block type for the file info block, a small key-value map of metadata
- **On-disk Size Without Header**: 4 bytes, integer, compressed size of the block's data, not including the header. Can
  be used for skipping the current data block when scanning HFile data.
- **Uncompressed Size Without Header**: 4 bytes, integer, uncompressed size of the block's data, not including the
  header. This is equal to the compressed size if the compression algorithm is NONE.
- **Previous Block Offset**: 8 bytes, long, file offset of the previous block of the same type. Can be used for seeking
  to the previous data/index block.
- **Checksum Type**: 1 byte, type of checksum used.
- **Bytes Per Checksum**: 4 bytes, integer, number of data bytes covered per checksum.
- **On-disk Data Size With Header**: 4 bytes, integer, on disk data size with header.

Data:

- **Data**: Compressed data (or uncompressed data if the compression algorithm is NONE). The size is indicated in the
  header. The content varies across different types of blocks, which are discussed later in this document.

Checksum:

- **Checksum**: checksum of the data. The size of checksums is indicated by the header.

## Data Block

The "Data" part of the Data Block consists of one or multiple key-value pairs, with keys sorted in lexicographical
order:

```
+--------------------+
|  Key-value Pair 0  |
+--------------------+
|  Key-value Pair 1  |
+--------------------+
|        ...         |
+--------------------+
| Key-value Pair N-1 |
+--------------------+
```

Each key-value pair has the following format:

```
 0                   1                   2                   3   
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                           Key Length                          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Value Length                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
~                              Key                              ~
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
~                             Value                             ~
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| MVCC Timestamp|
+-+-+-+-+-+-+-+-+
```

Header:

- **Key Length**: 4 bytes, integer, length of the key part.
- **Value Length**: 4 bytes, integer, lenghth of the value part.

Key:

```
 0                   1                   2                   3   
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|        Key Content Size       |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|                                                               |
~                          Key Content                          ~
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
~                       Other Information                       ~
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- **Key Content Size**: 2 byte, short, size of the key content.
- **Key Content**: key content in byte array. In Hudi, we serialize the String into byte array using UTF-8.
- **Other Information**: other information of the key, which is not used by Hudi.

Value:

The whole part represents the value in byte array. The size of value is indicated by the header.

MVCC Timestamp:

This is used by HBase and written to HFile. For Hudi, this field should always be zero, occupying 1 byte.

## Meta Block

The "Data" part of the Meta Block contains the meta information in byte array. The key of the meta block can be found in
the
Meta Index Block.

## Index Block

The "Data" part of the Index Block can be empty. When not empty, the "Data" part of Index Block contains one or more
block index entries organized like below:

```
+-----------------------+
|  Block Index Entry 0  |
+-----------------------+
|  Block Index Entry 1  |
+-----------------------+
|          ...          |
+-----------------------+
| Block Index Entry N-1 |
+-----------------------+
```

Each block index entry, referencing one relevant Data or Meta Block, has the following format:

```
 0                   1                   2                   3   
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
+                         Block Offset                          +
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      Block Size on Disk                       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
~                          Key Length                           ~
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
+                              Key                              +
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- **Block Offset**: 8 bytes, long, the start offset of a data or meta block in the file.
- **Block Size on Disk**: 4 bytes, integer, the on-disk size of the block, so the block can be skipped based on the
  size.
- **Key Length**: [variable-length encoded](https://en.wikipedia.org/wiki/Variable-length_quantity) number representing
  the length of the "Key" part.

Key:

```
+----------------+-----------+
| Key Bytes Size | Key Bytes |
+----------------+-----------+
```

For Data Index, the "Key Bytes" part has the following format (same as the key format in the Data Block):

```
 0                   1                   2                   3   
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|        Key Content Size       |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|                                                               |
~                          Key Content                          ~
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
~                       Other Information                       ~
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- **Key Content Size**: 2 byte, short, size of the key content.
- **Key Content**: key content in byte array. In Hudi, we encode the String into bytes using UTF-8.
- **Other Information**: other information of the key, which is not used by Hudi.

For Meta Index, the "Key Bytes" part is the byte array of the key of the Meta Block.

## File Info Block

The "Data" part of the File Info Block has the following format:

```
 0                   1                   2                   3   
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                           PBUF Magic                          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
~                           File Info                           ~
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

```

- **PBUF Magic**: 4 bytes, magic bytes `PBUF` indicating the block is using Protobuf for serde.
- **File Info**: a small key-value map of metadata serialized in Protobuf.

Here's the definition of the File Info proto `InfoProto`:

```
message BytesBytesPair {
  required bytes first = 1;
  required bytes second = 2;
}

message InfoProto {
  repeated BytesBytesPair map_entry = 1;
}
```

The key and value are represented in byte array. When Hudi adds more key-value metadata entry to the file info, the key
and value are encoded from String into byte array using UTF-8.

Here are common metadata stored in the File Info Block:

- `hfile.LASTKEY`: The last key of the file (byte array)
- `hfile.MAX_MEMSTORE_TS_KEY`: Maximum MVCC timestamp of the key-value pairs in the file. In Hudi, this should always be
    0.

## Trailer

The HFile Trailer has a fixed size, 4096 bytes. The HFile Trailer has different format compared to other blocks, as
follows:

```
 0                   1                   2                   3   
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
+                         Block Magic                           +
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
~                       Trailer Content                         ~
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- **Block Magic**: 8 bytes, a sequence of bytes indicating the Trailer, i.e., `TRABLK"$`.
- **Trailer Content**: the metadata fields are serialized in Protobuf, defined as follows

```
message TrailerProto {
  optional uint64 file_info_offset = 1;
  optional uint64 load_on_open_data_offset = 2;
  optional uint64 uncompressed_data_index_size = 3;
  optional uint64 total_uncompressed_bytes = 4;
  optional uint32 data_index_count = 5;
  optional uint32 meta_index_count = 6;
  optional uint64 entry_count = 7;
  optional uint32 num_data_index_levels = 8;
  optional uint64 first_data_block_offset = 9;
  optional uint64 last_data_block_offset = 10;
  optional string comparator_class_name = 11;
  optional uint32 compression_codec = 12;
  optional bytes encryption_key = 13;
}
```

Here are the meaning of each field:

- `file_info_offset`: File info offset
- `load_on_open_data_offset`: The offset of the section ("Load-on-open" section) that we need to load when opening the
  file
- `uncompressed_data_index_size`: The total uncompressed size of the whole data block index
- `total_uncompressed_bytes`: Total uncompressed bytes
- `data_index_count`: Number of data index entries
- `meta_index_count`: Number of meta index entries
- `entry_count`: Number of key-value pair entries in the file
- `num_data_index_levels`: The number of levels in the data block index
- `first_data_block_offset`: The offset of the first data block
- `last_data_block_offset`: The offset of the first byte after the last key-value data block
- `comparator_class_name`: Comparator class name (In Hudi, we always assume lexicographical order, so this is ignored)
- `compression_codec`: Compression codec: 0 = LZO, 1 = GZ, 2 = NONE
- `encryption_key`: Encryption key (not used by Hudi)

The last 4 bytes of the Trailer content contain the HFile version: the number represented by the first byte indicates
the minor version, and the number represented by the last three bytes indicates the major version. In the case of Hudi,
the major version should always be 3, if written by HBase HFile writer.
