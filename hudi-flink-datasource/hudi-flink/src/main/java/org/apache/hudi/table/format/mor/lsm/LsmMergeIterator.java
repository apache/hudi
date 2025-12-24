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

package org.apache.hudi.table.format.mor.lsm;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.lsm.MergeSorter;
import org.apache.hudi.io.lsm.RecordReader;
import org.apache.hudi.util.RowDataProjection;

import org.apache.avro.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class LsmMergeIterator implements ClosableIterator<RowData> {

  private final Iterator<RowData> iterator;
  private final RowDataProjection projection;
  private RecordReader<RowData> reader;
  private boolean skipProject;

  public LsmMergeIterator(
      boolean isIgnoreDelete,
      Configuration flinkConf,
      TypedProperties payloadProps,
      Schema requiredSchemaWithMeta,
      RowType oriRequiredRowType,
      org.apache.hadoop.conf.Configuration hadoopConf,
      int[] oriRequiredPos,
      String tablePath,
      List<RecordReader<HoodieRecord>> readers,
      boolean skipProject) throws IOException { // the iterator should be with full schema
    List<String> mergers = Arrays.stream(flinkConf.getString(FlinkOptions.RECORD_MERGER_IMPLS).split(","))
        .map(String::trim)
        .distinct()
        .collect(Collectors.toList());
    HoodieRecordMerger recordMerger = HoodieRecordUtils.createRecordMerger(tablePath,
        EngineType.FLINK, mergers, flinkConf.getString(FlinkOptions.RECORD_MERGER_STRATEGY));
    this.reader = new MergeSorter().mergeSort(readers, new FlinkRecordMergerWrapper(isIgnoreDelete, recordMerger,
        requiredSchemaWithMeta, requiredSchemaWithMeta, payloadProps), FlinkLsmUtils.getHoodieFlinkRecordComparator());
    this.iterator = reader.read();
    int[] projectPos = getProjectPos(oriRequiredPos, oriRequiredRowType, requiredSchemaWithMeta);
    this.projection = RowDataProjection.instance(oriRequiredRowType, projectPos);
    this.skipProject = skipProject;
  }

  private int[] getProjectPos(int[] requiredPos, RowType oriRequiredRowType, Schema requiredSchemaWithMeta) {
    int[] res = new int[requiredPos.length];
    List<String> oriFieldNames = oriRequiredRowType.getFieldNames();
    ValidationUtils.checkArgument(oriFieldNames.size() == requiredPos.length);
    List<String> fieldNameWithMeta = requiredSchemaWithMeta.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
    List<Integer> indexes = oriFieldNames.stream().map(fieldNameWithMeta::indexOf).collect(Collectors.toList());

    for (int i = 0; i < indexes.size(); i++) {
      res[i] = indexes.get(i);
    }
    return res;
  }

  @Override
  public void close() {
    try {
      if (reader != null) {
        reader.close();
      }
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    } finally {
      reader = null;
    }
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public RowData next() {
    RowData record = iterator.next();
    if (record == null) {
      return null;
    }
    if (skipProject) {
      return record;
    }
    return projection.project(record);
  }
}
