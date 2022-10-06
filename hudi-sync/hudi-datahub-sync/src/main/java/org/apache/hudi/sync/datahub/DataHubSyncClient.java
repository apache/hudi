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

import com.linkedin.common.Status;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.sync.common.HoodieSyncClient;
import org.apache.hudi.sync.common.HoodieSyncException;
import org.apache.hudi.sync.datahub.config.DataHubSyncConfig;

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
import org.apache.avro.Schema;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class DataHubSyncClient extends HoodieSyncClient {

  protected final DataHubSyncConfig config;
  private final DatasetUrn datasetUrn;
  private static final Status SOFT_DELETE_FALSE = new Status().setRemoved(false);

  public DataHubSyncClient(DataHubSyncConfig config) {
    super(config);
    this.config = config;
    this.datasetUrn = config.datasetIdentifier.getDatasetUrn();
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
  public void updateTableProperties(String tableName, Map<String, String> tableProperties) {
    MetadataChangeProposalWrapper propertiesChangeProposal = MetadataChangeProposalWrapper.builder()
        .entityType("dataset")
        .entityUrn(datasetUrn)
        .upsert()
        .aspect(new DatasetProperties().setCustomProperties(new StringMap(tableProperties)))
        .build();

    DatahubResponseLogger responseLogger = new DatahubResponseLogger();

    try (RestEmitter emitter = config.getRestEmitter()) {
      emitter.emit(propertiesChangeProposal, responseLogger).get();
    } catch (Exception e) {
      throw new HoodieDataHubSyncException("Fail to change properties for Dataset " + datasetUrn + ": " +
              tableProperties, e);
    }
  }

  @Override
  public void updateTableSchema(String tableName, MessageType schema) {
    Schema avroSchema = getAvroSchemaWithoutMetadataFields(metaClient);
    List<SchemaField> fields = avroSchema.getFields().stream().map(f -> new SchemaField()
        .setFieldPath(f.name())
        .setType(toSchemaFieldDataType(f.schema().getType()))
        .setDescription(f.doc(), SetMode.IGNORE_NULL)
        .setNativeDataType(f.schema().getType().getName())).collect(Collectors.toList());

    final SchemaMetadata.PlatformSchema platformSchema = new SchemaMetadata.PlatformSchema();
    platformSchema.setOtherSchema(new OtherSchema().setRawSchema(avroSchema.toString()));
    MetadataChangeProposalWrapper schemaChange = createSchemaMetadataUpdate(tableName, fields, platformSchema);

    DatahubResponseLogger responseLogger = new DatahubResponseLogger();
    try (RestEmitter emitter = config.getRestEmitter()) {
      emitter.emit(schemaChange, responseLogger).get();
      undoSoftDelete(emitter, responseLogger);
    } catch (Exception e) {
      throw new HoodieDataHubSyncException("Fail to change schema for Dataset " + datasetUrn, e);
    }
  }

  @Override
  public Map<String, String> getMetastoreSchema(String tableName) {
    throw new UnsupportedOperationException("Not supported: `getTableSchema`");
  }

  @Override
  public void close() {
    // no op;
  }

  // When updating an entity, it is ncessary to set its soft-delete status to false, or else the update won't get
  // reflected in the UI.
  private void undoSoftDelete(RestEmitter client, DatahubResponseLogger responseLogger) throws IOException, ExecutionException,
          InterruptedException {
    MetadataChangeProposalWrapper softDeleteUndoProposal = MetadataChangeProposalWrapper.builder()
            .entityType("dataset")
            .entityUrn(datasetUrn)
            .upsert()
            .aspect(SOFT_DELETE_FALSE)
            .aspectName("status")
            .build();

    client.emit(softDeleteUndoProposal, responseLogger).get();
  }

  private MetadataChangeProposalWrapper createSchemaMetadataUpdate(String tableName, List<SchemaField> fields,
                                                                   SchemaMetadata.PlatformSchema platformSchema) {
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

  static Schema getAvroSchemaWithoutMetadataFields(HoodieTableMetaClient metaClient) {
    try {
      return new TableSchemaResolver(metaClient).getTableAvroSchema(true);
    } catch (Exception e) {
      throw new HoodieSyncException("Failed to read avro schema", e);
    }
  }

  static SchemaFieldDataType toSchemaFieldDataType(Schema.Type type) {
    switch (type) {
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
        throw new AvroTypeException("Unexpected type: " + type.getName());
    }
  }
}
