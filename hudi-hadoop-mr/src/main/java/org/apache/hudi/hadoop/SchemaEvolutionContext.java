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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.TablePathUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.realtime.AbstractRealtimeRecordReader;
import org.apache.hudi.hadoop.realtime.RealtimeSplit;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.action.InternalSchemaMerger;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.internal.schema.utils.InternalSchemaUtils;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class is responsible for calculating names and types of fields that are actual at a certain point in time for hive.
 * If field is renamed in queried schema, its old name will be returned, which is relevant at the provided time.
 * If type of field is changed, its old type will be returned, and projection will be created that will convert the old type to the queried one.
 */
public class SchemaEvolutionContext {

  private static final Logger LOG = LogManager.getLogger(SchemaEvolutionContext.class);

  private static final String HIVE_TMP_READ_COLUMN_NAMES_CONF_STR = "hive.tmp.io.file.readcolumn.ids";
  private static final String HIVE_TMP_COLUMNS = "hive.tmp.columns";
  private static final String HIVE_EVOLUTION_ENABLE = "hudi.hive.schema.evolution";

  private final InputSplit split;
  private final JobConf job;
  private HoodieTableMetaClient metaClient;
  public Option<InternalSchema> internalSchemaOption;

  public SchemaEvolutionContext(InputSplit split, JobConf job) throws IOException {
    this(split, job, Option.empty());
  }

  public SchemaEvolutionContext(InputSplit split, JobConf job, Option<HoodieTableMetaClient> metaClientOption) throws IOException {
    this.split = split;
    this.job = job;
    this.metaClient = metaClientOption.isPresent() ? metaClientOption.get() : setUpHoodieTableMetaClient();
    if (this.metaClient == null) {
      internalSchemaOption = Option.empty();
      return;
    }
    try {
      TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
      this.internalSchemaOption = schemaUtil.getTableInternalSchemaFromCommitMetadata();
    } catch (Exception e) {
      internalSchemaOption = Option.empty();
      LOG.warn(String.format("failed to get internal Schema from hudi table：%s", metaClient.getBasePathV2()), e);
    }
    LOG.info(String.format("finish init schema evolution for split: %s", split));
  }

  private HoodieTableMetaClient setUpHoodieTableMetaClient() throws IOException {
    try {
      Path inputPath = ((FileSplit)split).getPath();
      FileSystem fs =  inputPath.getFileSystem(job);
      Option<Path> tablePath = TablePathUtils.getTablePath(fs, inputPath);
      return HoodieTableMetaClient.builder().setBasePath(tablePath.get().toString()).setConf(job).build();
    } catch (Exception e) {
      LOG.warn(String.format("Not a valid hoodie table, table path: %s", ((FileSplit)split).getPath()), e);
      return null;
    }
  }

  /**
   * Do schema evolution for RealtimeInputFormat.
   *
   * @param realtimeRecordReader recordReader for RealtimeInputFormat.
   * @return
   */
  public void doEvolutionForRealtimeInputFormat(AbstractRealtimeRecordReader realtimeRecordReader) throws Exception {
    if (!(split instanceof RealtimeSplit)) {
      LOG.warn(String.format("expect realtime split for mor table, but find other type split %s", split));
      return;
    }
    if (internalSchemaOption.isPresent()) {
      Schema tableAvroSchema = new TableSchemaResolver(metaClient).getTableAvroSchema();
      List<String> requiredColumns = getRequireColumn(job);
      InternalSchema prunedInternalSchema = InternalSchemaUtils.pruneInternalSchema(internalSchemaOption.get(),
          requiredColumns);
      // Add partitioning fields to writer schema for resulting row to contain null values for these fields
      String partitionFields = job.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "");
      List<String> partitioningFields = partitionFields.length() > 0 ? Arrays.stream(partitionFields.split("/")).collect(Collectors.toList())
          : new ArrayList<>();
      Schema writerSchema = AvroInternalSchemaConverter.convert(internalSchemaOption.get(), tableAvroSchema.getName());
      writerSchema = HoodieRealtimeRecordReaderUtils.addPartitionFields(writerSchema, partitioningFields);
      Map<String, Schema.Field> schemaFieldsMap = HoodieRealtimeRecordReaderUtils.getNameToFieldMap(writerSchema);
      // we should get HoodieParquetInputFormat#HIVE_TMP_COLUMNS,since serdeConstants#LIST_COLUMNS maybe change by HoodieParquetInputFormat#setColumnNameList
      Schema hiveSchema = realtimeRecordReader.constructHiveOrderedSchema(writerSchema, schemaFieldsMap, job.get(HIVE_TMP_COLUMNS));
      Schema readerSchema = AvroInternalSchemaConverter.convert(prunedInternalSchema, tableAvroSchema.getName());
      // setUp evolution schema
      realtimeRecordReader.setWriterSchema(writerSchema);
      realtimeRecordReader.setReaderSchema(readerSchema);
      realtimeRecordReader.setHiveSchema(hiveSchema);
      RealtimeSplit realtimeSplit = (RealtimeSplit) split;
      LOG.info(String.format("About to read compacted logs %s for base split %s, projecting cols %s",
          realtimeSplit.getDeltaLogPaths(), realtimeSplit.getPath(), requiredColumns));
    }
  }

  /**
   * Do schema evolution for ParquetFormat.
   */
  public void doEvolutionForParquetFormat() {
    if (internalSchemaOption.isPresent()) {
      // reading hoodie schema evolution table
      job.setBoolean(HIVE_EVOLUTION_ENABLE, true);
      Path finalPath = ((FileSplit)split).getPath();
      InternalSchema prunedSchema;
      List<String> requiredColumns = getRequireColumn(job);
      // No need trigger schema evolution for count(*)/count(1) operation
      boolean disableSchemaEvolution =
          requiredColumns.isEmpty() || (requiredColumns.size() == 1 && requiredColumns.get(0).isEmpty());
      if (!disableSchemaEvolution) {
        prunedSchema = InternalSchemaUtils.pruneInternalSchema(internalSchemaOption.get(), requiredColumns);
        InternalSchema querySchema = prunedSchema;
        Long commitTime = Long.valueOf(FSUtils.getCommitTime(finalPath.getName()));
        InternalSchema fileSchema = InternalSchemaCache.searchSchemaAndCache(commitTime, metaClient, false);
        InternalSchema mergedInternalSchema = new InternalSchemaMerger(fileSchema, querySchema, true,
            true).mergeSchema();
        List<Types.Field> fields = mergedInternalSchema.columns();
        setColumnNameList(job, fields);
        setColumnTypeList(job, fields);
        pushDownFilter(job, querySchema, fileSchema);
      }
    }
  }

  public void setColumnTypeList(JobConf job, List<Types.Field> fields) {
    List<TypeInfo> fullTypeInfos = TypeInfoUtils.getTypeInfosFromTypeString(job.get(serdeConstants.LIST_COLUMN_TYPES));
    List<Integer> tmpColIdList = Arrays.stream(job.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR).split(","))
        .map(Integer::parseInt).collect(Collectors.toList());
    if (tmpColIdList.size() != fields.size()) {
      throw new HoodieException(String.format("The size of hive.io.file.readcolumn.ids: %s is not equal to projection columns: %s",
          job.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR), fields.stream().map(Types.Field::name).collect(Collectors.joining(","))));
    }
    List<TypeInfo> fieldTypes = new ArrayList<>();
    for (int i = 0; i < tmpColIdList.size(); i++) {
      Types.Field field = fields.get(i);
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfosFromTypeString(fullTypeInfos.get(tmpColIdList.get(i)).getQualifiedName()).get(0);
      TypeInfo fieldType = constructHiveSchemaFromType(field.type(), typeInfo);
      fieldTypes.add(fieldType);
    }
    for (int i = 0; i < tmpColIdList.size(); i++) {
      TypeInfo typeInfo = fieldTypes.get(i);
      if (!(typeInfo instanceof PrimitiveTypeInfo)) {
        int index = tmpColIdList.get(i);
        fullTypeInfos.remove(index);
        fullTypeInfos.add(index, typeInfo);
      }
    }
    List<String> fullColTypeList = TypeInfoUtils.getTypeStringsFromTypeInfo(fullTypeInfos);
    String fullColTypeListString = String.join(",", fullColTypeList);
    job.set(serdeConstants.LIST_COLUMN_TYPES, fullColTypeListString);
  }

  private TypeInfo constructHiveSchemaFromType(Type type, TypeInfo typeInfo) {
    switch (type.typeId()) {
      case RECORD:
        Types.RecordType record = (Types.RecordType)type;
        List<Types.Field> fields = record.fields();
        ArrayList<TypeInfo>  fieldTypes = new ArrayList<>();
        ArrayList<String>  fieldNames = new ArrayList<>();
        for (int index = 0; index < fields.size(); index++) {
          StructTypeInfo structTypeInfo = (StructTypeInfo)typeInfo;
          TypeInfo subTypeInfo = getSchemaSubTypeInfo(structTypeInfo.getAllStructFieldTypeInfos().get(index), fields.get(index).type());
          fieldTypes.add(subTypeInfo);
          String name = fields.get(index).name();
          fieldNames.add(name);
        }
        StructTypeInfo structTypeInfo = new StructTypeInfo();
        structTypeInfo.setAllStructFieldNames(fieldNames);
        structTypeInfo.setAllStructFieldTypeInfos(fieldTypes);
        return structTypeInfo;
      case ARRAY:
        ListTypeInfo listTypeInfo = (ListTypeInfo)typeInfo;
        Types.ArrayType array = (Types.ArrayType) type;
        TypeInfo subTypeInfo = getSchemaSubTypeInfo(listTypeInfo.getListElementTypeInfo(), array.elementType());
        listTypeInfo.setListElementTypeInfo(subTypeInfo);
        return listTypeInfo;
      case MAP:
        Types.MapType map = (Types.MapType)type;
        MapTypeInfo mapTypeInfo = (MapTypeInfo)typeInfo;
        TypeInfo keyType = getSchemaSubTypeInfo(mapTypeInfo.getMapKeyTypeInfo(), map.keyType());
        TypeInfo valueType = getSchemaSubTypeInfo(mapTypeInfo.getMapValueTypeInfo(), map.valueType());
        MapTypeInfo mapType = new MapTypeInfo();
        mapType.setMapKeyTypeInfo(keyType);
        mapType.setMapValueTypeInfo(valueType);
        return mapType;
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DATE:
      case TIMESTAMP:
      case STRING:
      case UUID:
      case FIXED:
      case BINARY:
      case DECIMAL:
        return typeInfo;
      case TIME:
        throw new UnsupportedOperationException(String.format("cannot convert %s type to hive", new Object[] { type }));
      default:
        LOG.error(String.format("cannot convert unknown type: %s to Hive", new Object[] { type }));
        throw new UnsupportedOperationException(String.format("cannot convert unknown type: %s to Hive", new Object[] { type }));
    }
  }

  private TypeInfo getSchemaSubTypeInfo(TypeInfo hoodieTypeInfo, Type hiveType) {
    TypeInfo subTypeInfo = TypeInfoUtils.getTypeInfosFromTypeString(hoodieTypeInfo.getQualifiedName()).get(0);
    TypeInfo typeInfo;
    if (subTypeInfo instanceof PrimitiveTypeInfo) {
      typeInfo = subTypeInfo;
    } else {
      typeInfo = constructHiveSchemaFromType(hiveType, subTypeInfo);
    }
    return typeInfo;
  }

  private void pushDownFilter(JobConf job, InternalSchema querySchema, InternalSchema fileSchema) {
    String filterExprSerialized = job.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (filterExprSerialized != null) {
      ExprNodeGenericFuncDesc filterExpr = SerializationUtilities.deserializeExpression(filterExprSerialized);
      LinkedList<ExprNodeDesc> exprNodes = new LinkedList<ExprNodeDesc>();
      exprNodes.add(filterExpr);
      while (!exprNodes.isEmpty()) {
        int size = exprNodes.size();
        for (int i = 0; i < size; i++) {
          ExprNodeDesc expr = exprNodes.poll();
          if (expr instanceof ExprNodeColumnDesc) {
            String oldColumn = ((ExprNodeColumnDesc)expr).getColumn();
            String newColumn = InternalSchemaUtils.reBuildFilterName(oldColumn, fileSchema, querySchema);
            ((ExprNodeColumnDesc)expr).setColumn(newColumn);
          }
          List<ExprNodeDesc> children = expr.getChildren();
          if (children != null) {
            exprNodes.addAll(children);
          }
        }
      }
      String filterText = filterExpr.getExprString();
      String serializedFilterExpr = SerializationUtilities.serializeExpression(filterExpr);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Pushdown initiated with filterText = " + filterText + ", filterExpr = "
            + filterExpr + ", serializedFilterExpr = " + serializedFilterExpr);
      }
      job.set(TableScanDesc.FILTER_TEXT_CONF_STR, filterText);
      job.set(TableScanDesc.FILTER_EXPR_CONF_STR, serializedFilterExpr);
    }
  }

  private void setColumnNameList(JobConf job, List<Types.Field> fields) {
    if (fields == null) {
      return;
    }
    List<String> tmpColIdList = Arrays.asList(job.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR).split(","));
    if (fields.size() != tmpColIdList.size()) {
      return;
    }
    StringBuilder readColumnNames = new StringBuilder();
    List<String> tmpColNameList = Arrays.asList(job.get(serdeConstants.LIST_COLUMNS).split(","));
    List<String> fullColNamelist = new ArrayList<String>(tmpColNameList);
    for (int index = 0; index < fields.size(); index++) {
      String colName = fields.get(index).name();
      if (readColumnNames.length() > 0) {
        readColumnNames.append(',');
      }
      readColumnNames.append(colName);
      int id = Integer.parseInt(tmpColIdList.get(index));
      if (!colName.equals(fullColNamelist.get(id))) {
        fullColNamelist.remove(id);
        fullColNamelist.add(id, colName);
      }
    }
    String readColumnNamesString = readColumnNames.toString();
    job.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, readColumnNamesString);
    String fullColNamelistString = String.join(",", fullColNamelist);
    job.set(serdeConstants.LIST_COLUMNS, fullColNamelistString);
  }

  public static List<String> getRequireColumn(JobConf jobConf) {
    String originColumnString = jobConf.get(HIVE_TMP_READ_COLUMN_NAMES_CONF_STR);
    if (StringUtils.isNullOrEmpty(originColumnString)) {
      jobConf.set(HIVE_TMP_READ_COLUMN_NAMES_CONF_STR, jobConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR));
    }
    String hoodieFullColumnString = jobConf.get(HIVE_TMP_COLUMNS);
    if (StringUtils.isNullOrEmpty(hoodieFullColumnString)) {
      jobConf.set(HIVE_TMP_COLUMNS, jobConf.get(serdeConstants.LIST_COLUMNS));
    }
    String tableColumnString = jobConf.get(HIVE_TMP_READ_COLUMN_NAMES_CONF_STR);
    List<String> tableColumns = Arrays.asList(tableColumnString.split(","));
    return new ArrayList<>(tableColumns);
  }
}