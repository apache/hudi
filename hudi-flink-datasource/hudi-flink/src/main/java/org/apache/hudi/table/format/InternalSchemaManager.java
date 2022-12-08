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

package org.apache.hudi.table.format;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.action.InternalSchemaMerger;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.internal.schema.utils.InternalSchemaUtils;
import org.apache.hudi.util.AvroSchemaConverter;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This class is responsible for calculating names and types of fields that are actual at a certain point in time.
 * If field is renamed in queried schema, its old name will be returned, which is relevant at the provided time.
 * If type of field is changed, its old type will be returned, and projection will be created that will convert the old type to the queried one.
 */
public class InternalSchemaManager implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final InternalSchemaManager DISABLED = new InternalSchemaManager(null, InternalSchema.getEmptyInternalSchema(), null, null);

  private final Configuration conf;
  private final InternalSchema querySchema;
  private final String validCommits;
  private final String tablePath;
  private transient org.apache.hadoop.conf.Configuration hadoopConf;

  public static InternalSchemaManager get(Configuration conf, HoodieTableMetaClient metaClient) {
    if (!OptionsResolver.isSchemaEvolutionEnabled(conf)) {
      return DISABLED;
    }
    Option<InternalSchema> internalSchema = new TableSchemaResolver(metaClient).getTableInternalSchemaFromCommitMetadata();
    if (!internalSchema.isPresent() || internalSchema.get().isEmptySchema()) {
      return DISABLED;
    }
    String validCommits = metaClient
        .getCommitsAndCompactionTimeline()
        .filterCompletedInstants()
        .getInstantsAsStream()
        .map(HoodieInstant::getFileName)
        .collect(Collectors.joining(","));
    return new InternalSchemaManager(conf, internalSchema.get(), validCommits, metaClient.getBasePathV2().toString());
  }

  public InternalSchemaManager(Configuration conf, InternalSchema querySchema, String validCommits, String tablePath) {
    this.conf = conf;
    this.querySchema = querySchema;
    this.validCommits = validCommits;
    this.tablePath = tablePath;
  }

  public InternalSchema getQuerySchema() {
    return querySchema;
  }

  InternalSchema getFileSchema(String fileName) {
    if (querySchema.isEmptySchema()) {
      return querySchema;
    }
    long commitInstantTime = Long.parseLong(FSUtils.getCommitTime(fileName));
    InternalSchema fileSchemaUnmerged = InternalSchemaCache.getInternalSchemaByVersionId(
        commitInstantTime, tablePath, getHadoopConf(), validCommits);
    if (querySchema.equals(fileSchemaUnmerged)) {
      return InternalSchema.getEmptyInternalSchema();
    }
    return new InternalSchemaMerger(fileSchemaUnmerged, querySchema, true, true).mergeSchema();
  }

  CastMap getCastMap(InternalSchema fileSchema, String[] queryFieldNames, DataType[] queryFieldTypes, int[] selectedFields) {
    Preconditions.checkArgument(!querySchema.isEmptySchema(), "querySchema cannot be empty");
    Preconditions.checkArgument(!fileSchema.isEmptySchema(), "fileSchema cannot be empty");

    CastMap castMap = new CastMap();
    Map<Integer, Integer> posProxy = getPosProxy(fileSchema, queryFieldNames);
    if (posProxy.isEmpty()) {
      castMap.setFileFieldTypes(queryFieldTypes);
      return castMap;
    }
    List<Integer> selectedFieldList = IntStream.of(selectedFields).boxed().collect(Collectors.toList());
    List<DataType> fileSchemaAsDataTypes = AvroSchemaConverter.convertToDataType(
        AvroInternalSchemaConverter.convert(fileSchema, "tableName")).getChildren();
    DataType[] fileFieldTypes = new DataType[queryFieldTypes.length];
    for (int i = 0; i < queryFieldTypes.length; i++) {
      Integer posOfChangedType = posProxy.get(i);
      if (posOfChangedType == null) {
        fileFieldTypes[i] = queryFieldTypes[i];
      } else {
        DataType fileType = fileSchemaAsDataTypes.get(posOfChangedType);
        fileFieldTypes[i] = fileType;
        int selectedPos = selectedFieldList.indexOf(i);
        if (selectedPos != -1) {
          castMap.add(selectedPos, fileType.getLogicalType(), queryFieldTypes[i].getLogicalType());
        }
      }
    }
    castMap.setFileFieldTypes(fileFieldTypes);
    return castMap;
  }

  String[] getFileFieldNames(InternalSchema fileSchema, String[] queryFieldNames) {
    Preconditions.checkArgument(!querySchema.isEmptySchema(), "querySchema cannot be empty");
    Preconditions.checkArgument(!fileSchema.isEmptySchema(), "fileSchema cannot be empty");

    Map<String, String> renamedCols = InternalSchemaUtils.collectRenameCols(fileSchema, querySchema);
    if (renamedCols.isEmpty()) {
      return queryFieldNames;
    }
    return Arrays.stream(queryFieldNames).map(name -> renamedCols.getOrDefault(name, name)).toArray(String[]::new);
  }

  private Map<Integer, Integer> getPosProxy(InternalSchema fileSchema, String[] queryFieldNames) {
    Map<Integer, Pair<Type, Type>> changedCols = InternalSchemaUtils.collectTypeChangedCols(querySchema, fileSchema);
    HashMap<Integer, Integer> posProxy = new HashMap<>(changedCols.size());
    List<String> fieldNameList = Arrays.asList(queryFieldNames);
    List<Types.Field> columns = querySchema.columns();
    changedCols.forEach((posInSchema, typePair) -> {
      String name = columns.get(posInSchema).name();
      int posInType = fieldNameList.indexOf(name);
      posProxy.put(posInType, posInSchema);
    });
    return Collections.unmodifiableMap(posProxy);
  }

  private org.apache.hadoop.conf.Configuration getHadoopConf() {
    if (hadoopConf == null) {
      hadoopConf = HadoopConfigurations.getHadoopConf(conf);
    }
    return hadoopConf;
  }
}
