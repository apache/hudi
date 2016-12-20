/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.hive.util;


import org.apache.hadoop.fs.Path;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;

public class CsvParquetWriter extends ParquetWriter<List<String>> {

    public CsvParquetWriter(Path file, MessageType schema) throws IOException {
        this(file, schema, false);
    }

    public CsvParquetWriter(Path file, MessageType schema, boolean enableDictionary)
        throws IOException {
        this(file, schema, CompressionCodecName.UNCOMPRESSED, enableDictionary);
    }

    public CsvParquetWriter(Path file, MessageType schema, CompressionCodecName codecName,
        boolean enableDictionary) throws IOException {
        super(file, new CsvWriteSupport(schema), codecName,
            DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, enableDictionary, false);
    }
}
