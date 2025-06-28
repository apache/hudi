/*
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
 */

package org.apache.hudi.sync.datahub;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.exception.InvalidUnionTypeException;
import org.apache.hudi.sync.common.HoodieSyncClient;
import org.apache.hudi.sync.common.HoodieSyncException;
import org.apache.hudi.sync.datahub.config.DataHubSyncConfig;

import com.linkedin.common.Status;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.schema.ArrayType;
import com.linkedin.schema.BooleanType;
import com.linkedin.schema.BytesType;
import com.linkedin.schema.EnumType;
import com.linkedin.schema.FixedType;
import com.linkedin.schema.MapType;
import com.linkedin.schema.NullType;
import com.linkedin.schema.NumberType;
import com.linkedin.schema.OtherSchema;
import com.linkedin.schema.RecordType;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import com.linkedin.schema.UnionType;
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.parquet.schema.MessageType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_DATABASE_NAME;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_TABLE_NAME;

public class DataHubSyncClient extends HoodieSyncClient {

  protected final DataHubSyncConfig config;
  private final DatasetUrn datasetUrn;

  private static final String SCHEMA_FIELD_PATH_VERSION_TOKEN = "[version=2.0]";
  private static final Status SOFT_DELETE_FALSE = new Status().setRemoved(false);

  public DataHubSyncClient(DataHubSyncConfig config, HoodieTableMetaClient metaClient) {
    super(config, metaClient);
    this.config = config;
    this.datasetUrn = config.datasetIdentifier.getDatasetUrn();
  }

  @Override
  public String getTableName() {
    return config.getStringOrDefault(META_SYNC_DATAHUB_TABLE_NAME, config.getString(META_SYNC_TABLE_NAME));
  }

  @Override
  public String getDatabaseName() {
    return config.getStringOrDefault(META_SYNC_DATAHUB_DATABASE_NAME, config.getString(META_SYNC_DATABASE_NAME));
  }

  @Override
  public Option<String> getLastCommitTimeSynced(String tableName) {
    throw new UnsupportedOperationException("Not supported: `getLastCommitTimeSynced`");
  }

  @Override
  public void updateLastCommitTimeSynced(String tableName) {
    updateTableProperties(tableName, Collections.singletonMap(HOODIE_LAST_COMMIT_TIME_SYNC, getActiveTimeline().lastInstant().get().getTimestamp()));
  }

  @Override
  public boolean updateTableProperties(String tableName, Map<String, String> tableProperties) {
    MetadataChangeProposalWrapper propertiesChangeProposal = MetadataChangeProposalWrapper.builder()
            .entityType("dataset")
            .entityUrn(datasetUrn)
            .upsert()
            .aspect(new DatasetProperties().setCustomProperties(new StringMap(tableProperties)))
            .build();

    DatahubResponseLogger responseLogger = new DatahubResponseLogger();

    try (RestEmitter emitter = config.getRestEmitter()) {
      emitter.emit(propertiesChangeProposal, responseLogger).get();
      return true;
    } catch (Exception e) {
      throw new HoodieDataHubSyncException("Fail to change properties for Dataset " + datasetUrn + ": "
              + tableProperties, e);
    }
  }

  @Override
  public void updateTableSchema(String tableName, MessageType schema) {
    try (RestEmitter emitter = config.getRestEmitter()) {
      DatahubResponseLogger responseLogger = new DatahubResponseLogger();
      MetadataChangeProposalWrapper schemaChange = createSchemaMetadataUpdate(tableName);
      emitter.emit(schemaChange, responseLogger).get();

      // When updating an entity, it is necessary to set its soft-delete status to false, or else the update won't get
      // reflected in the UI.
      MetadataChangeProposalWrapper softDeleteUndoProposal = createUndoSoftDelete();
      emitter.emit(softDeleteUndoProposal, responseLogger).get();
    } catch (Exception e) {
      throw new HoodieDataHubSyncException("Fail to change schema for Dataset " + datasetUrn, e);
    }
  }

  @Override
  public Map<String, String> getMetastoreSchema(String tableName) {
    throw new UnsupportedOperationException("Not supported: `getMetastoreSchema`");
  }

  @Override
  public void close() {
    // no op;
  }

  private MetadataChangeProposalWrapper createUndoSoftDelete() {
    MetadataChangeProposalWrapper softDeleteUndoProposal = MetadataChangeProposalWrapper.builder()
            .entityType("dataset")
            .entityUrn(datasetUrn)
            .upsert()
            .aspect(SOFT_DELETE_FALSE)
            .aspectName("status")
            .build();
    return softDeleteUndoProposal;
  }

  private MetadataChangeProposalWrapper createSchemaMetadataUpdate(String tableName) {
    Schema avroSchema = getAvroSchemaWithoutMetadataFields(metaClient);
    List<SchemaField> fields = createSchemaFields(avroSchema);

    final SchemaMetadata.PlatformSchema platformSchema = new SchemaMetadata.PlatformSchema();
    platformSchema.setOtherSchema(new OtherSchema().setRawSchema(avroSchema.toString()));

    return MetadataChangeProposalWrapper.builder()
            .entityType("dataset")
            .entityUrn(datasetUrn)
            .upsert()
            .aspect(new SchemaMetadata()
                    .setSchemaName(tableName)
                    .setVersion(0)
                    .setHash("")
                    .setPlatform(datasetUrn.getPlatformEntity())
                    .setPlatformSchema(platformSchema)
                    .setFields(new SchemaFieldArray(fields)))
            .build();
  }

  Schema getAvroSchemaWithoutMetadataFields(HoodieTableMetaClient metaClient) {
    try {
      return new TableSchemaResolver(metaClient).getTableAvroSchema(true);
    } catch (Exception e) {
      throw new HoodieSyncException("Failed to read avro schema", e);
    }
  }

  /**
   * @see <a href="https://datahubproject.io/docs/advanced/field-path-spec-v2/">SchemaFieldPath Specification (Version 2)</a>
   * @param avroSchema
   * @return create list of datahub schema field from avro schema
   */
  @VisibleForTesting
  static List<SchemaField> createSchemaFields(Schema avroSchema) {
    List<SchemaField> schemaFields = new ArrayList<>();
    for (Schema.Field field : avroSchema.getFields()) {
      processField(schemaFields, SCHEMA_FIELD_PATH_VERSION_TOKEN, field);
    }
    return schemaFields;
  }

  private static void processField(
      List<SchemaField> schemaFields, String parentPath, Schema.Field field) {
    processField(schemaFields, parentPath, field, false);
  }

  /**
   * Converts an avro schema field to a datahub schema field
   */
  private static void processField(
      List<SchemaField> schemaFields, String parentPath, Schema.Field field, boolean isNullable) {
    Schema avroSchema = field.schema();
    Schema.Type avroType = avroSchema.getType();
    String fieldPath = parentPath + "." + field.name();
    switch (avroType) {
      case BOOLEAN:
      case INT:
      case FLOAT:
      case LONG:
      case DOUBLE:
      case FIXED:
      case ENUM:
      case STRING:
      case BYTES:
      case NULL:
        addSchemaField(schemaFields, fieldPath, avroSchema, field.doc(), isNullable);
        break;
      case MAP:
        addSchemaField(schemaFields, fieldPath, avroSchema, field.doc(), isNullable);
        processField(schemaFields, fieldPath, new Schema.Field("key", Schema.create(Schema.Type.STRING)));
        processField(schemaFields, fieldPath, new Schema.Field("value", avroSchema.getValueType()));
        break;
      case ARRAY:
        addSchemaField(schemaFields, fieldPath, avroSchema, field.doc(), isNullable);
        processField(schemaFields, fieldPath, new Schema.Field("element", avroSchema.getElementType()));
        break;
      case RECORD:
        addSchemaField(schemaFields, fieldPath, avroSchema, field.doc(), isNullable);
        for (Schema.Field recordField : avroSchema.getFields()) {
          processField(schemaFields, fieldPath, recordField);
        }
        break;
      case UNION:
        if (avroSchema.getTypes().size() == 1) {
          // Avro supports union with single type as well so use actual type in that case
          processField(schemaFields, parentPath, new Schema.Field(field.name(), avroSchema.getTypes().get(0), field.doc()));
        } else if (avroSchema.isNullable()) {
          // In case of a union having null, remove it and mark field as nullable
          List<Schema> remainingTypes = avroSchema.getTypes().stream()
              .filter(t -> !t.getType().equals(Schema.Type.NULL))
              .collect(Collectors.toList());
          // remainingTypes won't be empty at this point as avro does not allow duplicate
          // "null" type in union schema: https://avro.apache.org/docs/1.11.1/specification/#unions
          // throwing error to avoid any invalid state
          if (remainingTypes.isEmpty()) {
            throw new InvalidUnionTypeException("Duplicate null type in union: " + avroSchema.getTypes());
          }
          if (remainingTypes.size() == 1) {
            processField(schemaFields, parentPath, new Schema.Field(field.name(), remainingTypes.get(0), field.doc()), true);
          } else {
            processField(schemaFields, parentPath, new Schema.Field(field.name(), Schema.createUnion(remainingTypes), field.doc()), true);
          }
        } else {
          addSchemaField(schemaFields, fieldPath, avroSchema, field.doc(), isNullable);
          for (Schema unionSchema : avroSchema.getTypes()) {
            if (!unionSchema.isNullable()) {
              processField(schemaFields, fieldPath, new Schema.Field(unionSchema.getName(), unionSchema, unionSchema.getDoc()));
            }
          }
        }
        break;
      default:
        throw new AvroTypeException("Unexpected type: " + avroType.getName());
    }
  }

  private static void addSchemaField(
      List<SchemaField> schemaFields, String fieldPath, Schema avroSchema, String doc, boolean isNullable) {
    SchemaField schemaField = new SchemaField()
        .setFieldPath(fieldPath)
        .setDescription(doc, SetMode.IGNORE_NULL)
        .setNullable(isNullable)
        .setNativeDataType(toSchemaFieldNativeDataType(avroSchema))
        .setType(toSchemaFieldDataType(avroSchema.getType()));
    schemaFields.add(schemaField);
  }

  /**
   * @param avroType avro data type
   * @return SchemaFieldDataType for given avro field schema
   */
  @VisibleForTesting
  static SchemaFieldDataType toSchemaFieldDataType(Schema.Type avroType) {
    switch (avroType) {
      case BOOLEAN:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BooleanType()));
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType()));
      case MAP:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new MapType()));
      case ENUM:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new EnumType()));
      case NULL:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NullType()));
      case ARRAY:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new ArrayType()));
      case BYTES:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BytesType()));
      case FIXED:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new FixedType()));
      case UNION:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new UnionType()));
      case RECORD:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType()));
      case STRING:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType()));
      default:
        throw new AvroTypeException("Unexpected type: " + avroType.getName());
    }
  }

  /**
   * <li>timestamp with no timezone or global timestamp will be represented as "timestamp"</li>
   * <li>timestamp in a local timezone will be represented as "local-timestamp"</li>
   * @param schema avro field schema
   * @return native data type for given avro field schema
   */
  private static String toSchemaFieldNativeDataType(Schema schema) {
    LogicalType logicalType = schema.getLogicalType();
    if (logicalType != null) {
      if (logicalType instanceof LogicalTypes.Decimal) {
        LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
        return String.format("decimal(%s,%s)", decimalType.getPrecision(), decimalType.getScale());
      } else {
        return logicalType.getName();
      }
    }
    return schema.getType().getName();
  }
}
