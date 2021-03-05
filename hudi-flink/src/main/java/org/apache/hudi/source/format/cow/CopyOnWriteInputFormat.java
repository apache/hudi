/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.source.format.cow;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.utils.SerializableConfiguration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.flink.table.data.vector.VectorizedColumnBatch.DEFAULT_SIZE;
import static org.apache.flink.table.filesystem.RowPartitionComputer.restorePartValueFromType;

/**
 * An implementation of {@link FileInputFormat} to read {@link RowData} records
 * from Parquet files.
 *
 * <p>Note: Reference Flink release 1.11.2
 * {@code org.apache.flink.formats.parquet.ParquetFileSystemFormatFactory.ParquetInputFormat}
 * to support TIMESTAMP_MILLIS.
 *
 * @see ParquetSplitReaderUtil
 */
public class CopyOnWriteInputFormat extends FileInputFormat<RowData> {
  private static final long serialVersionUID = 1L;

  private final String[] fullFieldNames;
  private final DataType[] fullFieldTypes;
  private final int[] selectedFields;
  private final String partDefaultName;
  private final boolean utcTimestamp;
  private final SerializableConfiguration conf;
  private final long limit;

  private transient ParquetColumnarRowSplitReader reader;
  private transient long currentReadCount;

  public CopyOnWriteInputFormat(
      Path[] paths,
      String[] fullFieldNames,
      DataType[] fullFieldTypes,
      int[] selectedFields,
      String partDefaultName,
      long limit,
      Configuration conf,
      boolean utcTimestamp) {
    super.setFilePaths(paths);
    this.limit = limit;
    this.partDefaultName = partDefaultName;
    this.fullFieldNames = fullFieldNames;
    this.fullFieldTypes = fullFieldTypes;
    this.selectedFields = selectedFields;
    this.conf = new SerializableConfiguration(conf);
    this.utcTimestamp = utcTimestamp;
  }

  @Override
  public void open(FileInputSplit fileSplit) throws IOException {
    // generate partition specs.
    List<String> fieldNameList = Arrays.asList(fullFieldNames);
    LinkedHashMap<String, String> partSpec = PartitionPathUtils.extractPartitionSpecFromPath(
        fileSplit.getPath());
    LinkedHashMap<String, Object> partObjects = new LinkedHashMap<>();
    partSpec.forEach((k, v) -> partObjects.put(k, restorePartValueFromType(
        partDefaultName.equals(v) ? null : v,
        fullFieldTypes[fieldNameList.indexOf(k)])));

    this.reader = ParquetSplitReaderUtil.genPartColumnarRowReader(
        utcTimestamp,
        true,
        conf.conf(),
        fullFieldNames,
        fullFieldTypes,
        partObjects,
        selectedFields,
        DEFAULT_SIZE,
        new Path(fileSplit.getPath().toString()),
        fileSplit.getStart(),
        fileSplit.getLength());
    this.currentReadCount = 0L;
  }

  @Override
  public boolean supportsMultiPaths() {
    return true;
  }

  @Override
  public boolean reachedEnd() throws IOException {
    if (currentReadCount >= limit) {
      return true;
    } else {
      return reader.reachedEnd();
    }
  }

  @Override
  public RowData nextRecord(RowData reuse) {
    currentReadCount++;
    return reader.nextRecord();
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      this.reader.close();
    }
    this.reader = null;
  }
}
