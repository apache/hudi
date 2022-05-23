/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.format;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.action.InternalSchemaMerger;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.RowDataCastProjection;
import org.apache.hudi.util.RowDataProjection;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS;

/**
 * This class is responsible for calculating names and types of fields that are actual at a certain point in time.
 * If field is renamed in queried schema, its old name will be returned, which is relevant at the provided time.
 * If type of field is changed, its old type will be returned, and projection will be created that will convert the old type to the queried one.
 */
public final class SchemaEvoContext implements Serializable {
  private final InternalSchema querySchema;
  private final HoodieTableMetaClient metaClient;
  private List<String> fieldNames;
  private List<DataType> fieldTypes;
  private RowDataProjection projection;

  public SchemaEvoContext(InternalSchema querySchema, HoodieTableMetaClient metaClient) {
    this.querySchema = querySchema;
    this.metaClient = metaClient;
  }

  public void evalActualFields(String fileName, int[] selectedFields) {
    InternalSchema mergedSchema = getMergedSchema(fileName);
    List<DataType> fieldTypesWithMeta = AvroSchemaConverter.convertToDataType(AvroInternalSchemaConverter.convert(mergedSchema, tableName())).getChildren();
    List<Integer> selectedFieldsList = getSelectedFields(selectedFields);
    RowDataProjection projection = getProjection(mergedSchema, fieldTypesWithMeta, selectedFieldsList);
    List<String> fieldNamesWithMeta = mergedSchema.columns().stream().map(Types.Field::name).collect(Collectors.toList());
    setActualFields(fieldNamesWithMeta, fieldTypesWithMeta, projection);
  }

  public String[] fieldNames() {
    return fieldNames.toArray(new String[0]);
  }

  public DataType[] fieldTypes() {
    return fieldTypes.toArray(new DataType[0]);
  }

  public Option<RowDataProjection> projection() {
    return Option.ofNullable(projection);
  }

  private List<Integer> getSelectedFields(int[] selectedFields) {
    return Arrays.stream(selectedFields)
            .boxed()
            .map(pos -> pos + HOODIE_META_COLUMNS.size())
            .collect(Collectors.toList());
  }

  private InternalSchema getMergedSchema(String fileName) {
    long commitTime = Long.parseLong(FSUtils.getCommitTime(fileName));
    InternalSchema fileSchema = InternalSchemaCache.searchSchemaAndCache(commitTime, metaClient, false);
    return new InternalSchemaMerger(fileSchema, querySchema, true, true).mergeSchema();
  }

  private RowDataProjection getProjection(InternalSchema mergedSchema, List<DataType> actualFieldTypesWithMeta, List<Integer> selectedFields) {
    CastMap castMap = CastMap.of(tableName(), querySchema, mergedSchema);
    if (castMap.containsAnyPos(selectedFields)) {
      List<LogicalType> readType = new ArrayList<>(selectedFields.size());
      for (int pos : selectedFields) {
        readType.add(actualFieldTypesWithMeta.get(pos).getLogicalType());
      }
      return new RowDataCastProjection(
              readType.toArray(new LogicalType[0]),
              IntStream.range(0, selectedFields.size()).toArray(),
              castMap.rearrange(selectedFields, IntStream.range(0, selectedFields.size()).boxed().collect(Collectors.toList()))
      );
    }
    return null;
  }

  private void setActualFields(List<String> fieldNames, List<DataType> fieldTypes, RowDataProjection projection) {
    this.fieldNames = fieldNames.subList(HOODIE_META_COLUMNS.size(), fieldNames.size());
    this.fieldTypes = fieldTypes.subList(HOODIE_META_COLUMNS.size(), fieldTypes.size());
    this.projection = projection;
  }

  private String tableName() {
    return metaClient.getTableConfig().getTableName();
  }
}
