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

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.action.InternalSchemaMerger;
import org.apache.hudi.internal.schema.convert.InternalSchemaConverter;
import org.apache.hudi.internal.schema.utils.InternalSchemaUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.util.AvroSchemaConverter;

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

  public static final InternalSchemaManager DISABLED = new InternalSchemaManager(null, InternalSchema.getEmptyInternalSchema(), null, null,
      TimelineLayout.fromVersion(TimelineLayoutVersion.CURR_LAYOUT_VERSION), null);

  private final InternalSchema querySchema;
  private final String validCommits;
  private final String tablePath;
  private final TimelineLayout layout;
  private final HoodieTableConfig tableConfig;
  private final StorageConfiguration<?> storageConf;

  public static InternalSchemaManager get(StorageConfiguration<?> conf, HoodieTableMetaClient metaClient) {
    if (!isSchemaEvolutionEnabled(conf)) {
      return DISABLED;
    }
    Option<InternalSchema> internalSchema = new TableSchemaResolver(metaClient).getTableInternalSchemaFromCommitMetadata();
    if (!internalSchema.isPresent() || internalSchema.get().isEmptySchema()) {
      return DISABLED;
    }

    InstantFileNameGenerator factory = metaClient.getInstantFileNameGenerator();
    String validCommits = metaClient
        .getCommitsAndCompactionTimeline()
        .filterCompletedInstants()
        .getInstantsAsStream()
        .map(factory::getFileName)
        .collect(Collectors.joining(","));
    return new InternalSchemaManager(conf, internalSchema.get(), validCommits, metaClient.getBasePath().toString(), metaClient.getTimelineLayout(), metaClient.getTableConfig());
  }

  public InternalSchemaManager(StorageConfiguration<?> storageConf, InternalSchema querySchema, String validCommits, String tablePath,
                               TimelineLayout layout, HoodieTableConfig tableConfig) {
    this.storageConf = storageConf;
    this.querySchema = querySchema;
    this.validCommits = validCommits;
    this.tablePath = tablePath;
    this.layout = layout;
    this.tableConfig = tableConfig;
  }

  public InternalSchema getQuerySchema() {
    return querySchema;
  }

  /**
   * Attempts to merge the file and query schema to produce a mergeSchema, prioritising the use of fileSchema types.
   * An emptySchema is returned if:
   * <ul>
   * <li>1. An empty querySchema is provided</li>
   * <li>2. querySchema is equal to fileSchema</li>
   * </ul>
   * Note that this method returns an emptySchema if merging is not required to be performed.
   * @param fileName Name of file to fetch commitTime/versionId for
   * @return mergeSchema, i.e. the schema on which the file should be read with
   */
  InternalSchema getMergeSchema(String fileName) {
    if (querySchema.isEmptySchema()) {
      return querySchema;
    }
    long commitInstantTime = Long.parseLong(FSUtils.getCommitTime(fileName));
    InternalSchema fileSchema = InternalSchemaCache.getInternalSchemaByVersionId(
        commitInstantTime, tablePath,
        new HoodieHadoopStorage(tablePath, storageConf),
        validCommits, layout, tableConfig);
    if (querySchema.equals(fileSchema)) {
      return InternalSchema.getEmptyInternalSchema();
    }
    return new InternalSchemaMerger(fileSchema, querySchema, true, true).mergeSchema();
  }

  /**
   * This method returns a mapping of columns that have type inconsistencies between the mergeSchema and querySchema.
   * This is done by:
   * <li>1. Finding the columns with type changes</li>
   * <li>2. Get a map storing the index of these columns with type changes; Map of -> (colIdxInQueryFieldNames, colIdxInQuerySchema)</li>
   * <li>3. For each selectedField with type changes, build a castMap containing the cast/conversion details;
   * Map of -> (selectedPos, Cast([from] fileType, [to] queryType))</li>
   *
   * @param mergeSchema InternalSchema representation of mergeSchema (prioritise use of fileSchemaType) that is used for reading base parquet files
   * @param queryFieldNames array containing the columns of a Hudi Flink table
   * @param queryFieldTypes array containing the field types of the columns of a Hudi Flink table
   * @param selectedFields array containing the index of the columns of interest required (indexes are based on queryFieldNames and queryFieldTypes)
   * @return a castMap containing the information of how to cast a selectedField from the fileType to queryType.
   *
   * @see CastMap
   */
  CastMap getCastMap(InternalSchema mergeSchema, String[] queryFieldNames, DataType[] queryFieldTypes, int[] selectedFields) {
    Preconditions.checkArgument(!querySchema.isEmptySchema(), "querySchema cannot be empty");
    Preconditions.checkArgument(!mergeSchema.isEmptySchema(), "mergeSchema cannot be empty");

    CastMap castMap = new CastMap();
    // map storing the indexes of columns with type changes Map of -> (colIdxInQueryFieldNames, colIdxInQuerySchema)
    Map<Integer, Integer> posProxy = getPosProxy(mergeSchema, queryFieldNames);
    if (posProxy.isEmpty()) {
      // no type changes
      castMap.setFileFieldTypes(queryFieldTypes);
      return castMap;
    }
    List<Integer> selectedFieldList = IntStream.of(selectedFields).boxed().collect(Collectors.toList());
    // mergeSchema is built with useColumnTypeFromFileSchema = true
    List<DataType> mergeSchemaAsDataTypes = AvroSchemaConverter.convertToDataType(
        InternalSchemaConverter.convert(mergeSchema, "tableName").toAvroSchema()).getChildren();
    DataType[] fileFieldTypes = new DataType[queryFieldTypes.length];
    for (int i = 0; i < queryFieldTypes.length; i++) {
      // position of ChangedType in querySchema
      Integer posOfChangedType = posProxy.get(i);
      if (posOfChangedType == null) {
        // no type change for column; fileFieldType == queryFieldType
        fileFieldTypes[i] = queryFieldTypes[i];
      } else {
        // type change detected for column;
        DataType fileType = mergeSchemaAsDataTypes.get(posOfChangedType);
        // update fileFieldType match the type found in mergeSchema
        fileFieldTypes[i] = fileType;
        int selectedPos = selectedFieldList.indexOf(i);
        if (selectedPos != -1) {
          // if the column is part of user's query, add it into the castMap
          // castMap -> (position, Cast([from] fileType, [to] queryType))
          castMap.add(selectedPos, fileType.getLogicalType(), queryFieldTypes[i].getLogicalType());
        }
      }
    }
    castMap.setFileFieldTypes(fileFieldTypes);
    return castMap;
  }

  /**
   * For columns that have been modified via the column renaming operation, the column name might be inconsistent
   * between querySchema and mergeSchema.
   * <p>
   * As such, this method will identify all columns that have been renamed, and return a string array of column names
   * corresponding to the column names found in the mergeSchema.
   * <p>
   * This is done by:
   * <li>1. Get the rename mapping of -> (colNameFromNewSchema, colNameLastPartFromOldSchema)</li>
   * <li>2. For columns that have been renamed, replace them with the old column name</li>
   *
   * @param mergeSchema InternalSchema representation of mergeSchema (prioritise use of fileSchemaType) that is used for reading base parquet files
   * @param queryFieldNames array containing the columns of a Hudi Flink table
   * @return String array containing column names corresponding to the column names found in the mergeSchema
   *
   * @see InternalSchemaUtils#collectRenameCols(InternalSchema, InternalSchema)
   */
  String[] getMergeFieldNames(InternalSchema mergeSchema, String[] queryFieldNames) {
    Preconditions.checkArgument(!querySchema.isEmptySchema(), "querySchema cannot be empty");
    Preconditions.checkArgument(!mergeSchema.isEmptySchema(), "mergeSchema cannot be empty");

    Map<String, String> renamedCols = InternalSchemaUtils.collectRenameCols(mergeSchema, querySchema);
    if (renamedCols.isEmpty()) {
      return queryFieldNames;
    }
    return Arrays.stream(queryFieldNames).map(name -> renamedCols.getOrDefault(name, name)).toArray(String[]::new);
  }

  private Map<Integer, Integer> getPosProxy(InternalSchema mergeSchema, String[] queryFieldNames) {
    Map<Integer, Pair<Type, Type>> changedCols = InternalSchemaUtils.collectTypeChangedCols(querySchema, mergeSchema);
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

  /**
   * Returns whether comprehensive schema evolution enabled.
   */
  private static boolean isSchemaEvolutionEnabled(StorageConfiguration<?> conf) {
    return conf.getBoolean(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.defaultValue());
  }
}
