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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.SchemaDifference;
import org.apache.hudi.sync.common.HoodieSyncClient;
import org.apache.hudi.sync.common.HoodieSyncException;
import org.apache.hudi.sync.datahub.config.DataHubSyncConfig;
import org.apache.hudi.sync.datahub.config.HoodieDataHubDatasetIdentifier;
import org.apache.hudi.sync.datahub.util.SchemaFieldsUtil;

import com.linkedin.common.BrowsePathEntry;
import com.linkedin.common.BrowsePathEntryArray;
import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.Container;
import com.linkedin.container.ContainerProperties;
import com.linkedin.data.template.StringArray;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.aspect.patch.builder.DatasetPropertiesPatchBuilder;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import io.datahubproject.schematron.converters.avro.AvroSchemaConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataHubSyncClient extends HoodieSyncClient {

  private static final Logger LOG = LoggerFactory.getLogger(DataHubSyncClient.class);

  protected final DataHubSyncConfig config;
  private final DataPlatformUrn dataPlatformUrn;
  private final Option<String> dataPlatformInstance;
  private final Option<Urn> dataPlatformInstanceUrn;
  private final DatasetUrn datasetUrn;
  private final Urn databaseUrn;
  private final String tableName;
  private final String databaseName;
  private static final Status SOFT_DELETE_FALSE = new Status().setRemoved(false);

  public DataHubSyncClient(DataHubSyncConfig config, HoodieTableMetaClient metaClient) {
    super(config, metaClient);
    this.config = config;
    HoodieDataHubDatasetIdentifier datasetIdentifier =
            config.getDatasetIdentifier();
    this.dataPlatformUrn = datasetIdentifier.getDataPlatformUrn();
    this.dataPlatformInstance = datasetIdentifier.getDataPlatformInstance();
    this.dataPlatformInstanceUrn = datasetIdentifier.getDataPlatformInstanceUrn();
    this.datasetUrn = datasetIdentifier.getDatasetUrn();
    this.databaseUrn = datasetIdentifier.getDatabaseUrn();
    this.tableName = datasetIdentifier.getTableName();
    this.databaseName = datasetIdentifier.getDatabaseName();
  }

  @Override
  public String getDatabaseName() {
    return this.databaseName;
  }

  @Override
  public String getTableName() {
    return this.tableName;
  }

  @Override
  public Option<String> getLastCommitTimeSynced(String tableName) {
    throw new UnsupportedOperationException("Not supported: `getLastCommitTimeSynced`");
  }

  protected Option<String> getLastCommitTime() {
    return getActiveTimeline().lastInstant().map(HoodieInstant::requestedTime);
  }

  protected Option<String> getLastCommitCompletionTime() {
    return getActiveTimeline().getLatestCompletionTime();
  }

  @Override
  public void updateLastCommitTimeSynced(String tableName) {
    Option<String> lastCommitTime = getLastCommitTime();
    if (lastCommitTime.isPresent()) {
      updateTableProperties(tableName, Collections.singletonMap(HOODIE_LAST_COMMIT_TIME_SYNC, lastCommitTime.get()));
    } else {
      LOG.error("Failed to get last commit time");
    }

    Option<String> lastCommitCompletionTime = getLastCommitCompletionTime();
    if (lastCommitCompletionTime.isPresent()) {
      updateTableProperties(tableName, Collections.singletonMap(HOODIE_LAST_COMMIT_COMPLETION_TIME_SYNC, lastCommitCompletionTime.get()));
    } else {
      LOG.error("Failed to get last commit completion time");
    }
  }

  private MetadataChangeProposal createDatasetPropertiesAspect(String tableName, Map<String, String> tableProperties) {
    DatasetPropertiesPatchBuilder datasetPropertiesPatchBuilder = new DatasetPropertiesPatchBuilder().urn(datasetUrn);
    if (tableProperties != null) {
      tableProperties.forEach(datasetPropertiesPatchBuilder::addCustomProperty);
    }
    if (tableName != null) {
      datasetPropertiesPatchBuilder.setName(tableName);
    }
    return datasetPropertiesPatchBuilder.build();
  }

  @Override
  public boolean updateTableProperties(String tableName, Map<String, String> tableProperties) {
    // Use PATCH API to avoid overwriting existing properties
    MetadataChangeProposal proposal = createDatasetPropertiesAspect(tableName, tableProperties);
    DataHubResponseLogger responseLogger = new DataHubResponseLogger();

    try (RestEmitter emitter = config.getRestEmitter()) {
      Future<MetadataWriteResponse> future = emitter.emit(proposal, responseLogger);
      future.get();
      return true;
    } catch (Exception e) {
      if (!config.suppressExceptions()) {
        throw new HoodieDataHubSyncException(
                "Failed to sync properties for Dataset " + datasetUrn + ": " + tableProperties, e);
      } else {
        LOG.error("Failed to sync properties for Dataset {}: {}", datasetUrn, tableProperties, e);
        return false;
      }
    }
  }

  @Override
  public void updateTableSchema(String tableName, HoodieSchema schema, SchemaDifference schemaDifference) {
    try (RestEmitter emitter = config.getRestEmitter()) {
      DataHubResponseLogger responseLogger = new DataHubResponseLogger();

      Stream<MetadataChangeProposalWrapper> proposals =
              Stream.of(createContainerEntity(), createDatasetEntity()).flatMap(stream -> stream);

      // Execute all proposals in parallel and collect futures
      List<Future<MetadataWriteResponse>> futures = proposals.map(
              p -> {
                try {
                  return emitter.emit(p, responseLogger);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
      ).collect(Collectors.toList());

      List<MetadataWriteResponse> successfulResults = new ArrayList<>();
      List<Throwable> failures = new ArrayList<>();

      for (Future<MetadataWriteResponse> future : futures) {
        try {
          successfulResults.add(future.get(30, TimeUnit.SECONDS));
        } catch (TimeoutException e) {
          failures.add(new HoodieDataHubSyncException("Operation timed out", e));
        } catch (InterruptedException | ExecutionException e) {
          failures.add(e);
        }
      }

      if (!failures.isEmpty()) {
        if (!config.suppressExceptions()) {
          throw new HoodieDataHubSyncException("Failed to sync " + failures.size() + " operations", failures.get(0));
        } else {
          for (Throwable failure : failures) {
            LOG.error("Failed to sync operation", failure);
          }
        }
      }
    } catch (Exception e) {
      if (!config.suppressExceptions()) {
        throw new HoodieDataHubSyncException(String.format("Failed to sync metadata for dataset %s", tableName), e);
      } else {
        LOG.error("Failed to sync metadata for dataset {}", tableName, e);
      }
    }
  }

  private MetadataChangeProposalWrapper createContainerAspect(Urn entityUrn, Urn containerUrn) {
    MetadataChangeProposalWrapper containerProposal = MetadataChangeProposalWrapper.builder()
            .entityType(entityUrn.getEntityType())
            .entityUrn(entityUrn)
            .upsert()
            .aspect(new Container().setContainer(containerUrn))
            .build();
    return containerProposal;
  }

  private MetadataChangeProposalWrapper createBrowsePathsAspect(Urn entityUrn, List<BrowsePathEntry> paths) {
    BrowsePathEntryArray browsePathEntryArray = new BrowsePathEntryArray(paths);
    MetadataChangeProposalWrapper browsePathsProposal = MetadataChangeProposalWrapper.builder()
            .entityType(entityUrn.getEntityType())
            .entityUrn(entityUrn)
            .upsert()
            .aspect(new BrowsePathsV2().setPath(browsePathEntryArray))
            .build();
    return browsePathsProposal;
  }

  private MetadataChangeProposalWrapper createDataPlatformInstanceAspect(Urn entityUrn) {
    DataPlatformInstance dataPlatformInstanceAspect = new DataPlatformInstance().setPlatform(this.dataPlatformUrn);
    if (this.dataPlatformInstanceUrn.isPresent()) {
      dataPlatformInstanceAspect.setInstance(dataPlatformInstanceUrn.get());
    }

    MetadataChangeProposalWrapper dataPlatformInstanceProposal = MetadataChangeProposalWrapper.builder()
            .entityType(entityUrn.getEntityType())
            .entityUrn(entityUrn)
            .upsert()
            .aspect(dataPlatformInstanceAspect)
            .build();
    return dataPlatformInstanceProposal;
  }

  private MetadataChangeProposalWrapper createDomainAspect(Urn entityUrn) {
    try {
      Urn domainUrn = Urn.createFromString(config.getDomainIdentifier());
      MetadataChangeProposalWrapper attachDomainProposal = MetadataChangeProposalWrapper.builder()
              .entityType(entityUrn.getEntityType())
              .entityUrn(entityUrn)
              .upsert()
              .aspect(new Domains().setDomains(new UrnArray(domainUrn)))
              .build();
      return attachDomainProposal;
    } catch (URISyntaxException e) {
      LOG.warn("Failed to create domain URN from string: {}", config.getDomainIdentifier());
    }
    return null;
  }

  private Stream<MetadataChangeProposalWrapper> createContainerEntity() {
    MetadataChangeProposalWrapper containerEntityProposal = MetadataChangeProposalWrapper.builder()
            .entityType("container")
            .entityUrn(databaseUrn)
            .upsert()
            .aspect(new ContainerProperties().setName(databaseName))
            .build();

    List<BrowsePathEntry> paths = dataPlatformInstanceUrn.map(dpiUrn -> Collections.singletonList(
        new BrowsePathEntry().setUrn(dpiUrn).setId(dpiUrn.toString()))
    ).orElse(Collections.emptyList());

    Stream<MetadataChangeProposalWrapper> resultStream = Stream.of(
            containerEntityProposal,
            createSubTypeAspect(databaseUrn, "Database"),
            createDataPlatformInstanceAspect(databaseUrn),
            createBrowsePathsAspect(databaseUrn, paths),
            createStatusAspect(databaseUrn),
            config.attachDomain() ? createDomainAspect(databaseUrn) : null
        ).filter(Objects::nonNull);
    return resultStream;
  }

  @Override
  public Map<String, String> getMetastoreSchema(String tableName) {
    throw new UnsupportedOperationException("Not supported: `getMetastoreSchema`");
  }

  @Override
  public void close() {
    // no op;
  }

  private MetadataChangeProposalWrapper<Status> createStatusAspect(Urn urn) {
    MetadataChangeProposalWrapper<Status> softDeleteUndoProposal = MetadataChangeProposalWrapper.builder()
            .entityType(urn.getEntityType())
            .entityUrn(urn)
            .upsert()
            .aspect(SOFT_DELETE_FALSE)
            .build();
    return softDeleteUndoProposal;
  }

  private MetadataChangeProposalWrapper<SubTypes> createSubTypeAspect(Urn urn, String subType) {
    MetadataChangeProposalWrapper subTypeProposal = MetadataChangeProposalWrapper.builder()
            .entityType(urn.getEntityType())
            .entityUrn(urn)
            .upsert()
            .aspect(new SubTypes().setTypeNames(new StringArray(subType)))
            .build();
    return subTypeProposal;
  }

  private MetadataChangeProposalWrapper createSchemaMetadataAspect(String tableName) {
    HoodieSchema tableSchema = getAvroSchemaWithoutMetadataFields(metaClient);
    AvroSchemaConverter avroSchemaConverter = AvroSchemaConverter.builder().build();
    com.linkedin.schema.SchemaMetadata schemaMetadata = avroSchemaConverter.toDataHubSchema(
            tableSchema.toAvroSchema(),
            false,
            false,
            datasetUrn.getPlatformEntity(),
            null
    );

    // Reorder fields to relocate _hoodie_ metadata fields to the end
    schemaMetadata.setFields(SchemaFieldsUtil.reorderPrefixedFields(schemaMetadata.getFields(), "_hoodie_"));

    return MetadataChangeProposalWrapper.builder()
            .entityType("dataset")
            .entityUrn(datasetUrn)
            .upsert()
            .aspect(schemaMetadata)
            .build();
  }

  private Stream<MetadataChangeProposalWrapper> createDatasetEntity() {
    BrowsePathEntry databasePath = new BrowsePathEntry().setUrn(databaseUrn).setId(databaseUrn.toString());
    List<BrowsePathEntry> paths = dataPlatformInstanceUrn.map(dpiUrn -> {
      List<BrowsePathEntry> list = new ArrayList<BrowsePathEntry>();
      list.add(new BrowsePathEntry().setUrn(dpiUrn).setId(dpiUrn.toString()));
      list.add(databasePath);
      return list;
    }
    ).orElse(Collections.singletonList(databasePath));

    Stream<MetadataChangeProposalWrapper> result = Stream.of(
            createStatusAspect(datasetUrn),
            createSubTypeAspect(datasetUrn, "Table"),
            createDataPlatformInstanceAspect(datasetUrn),
            createBrowsePathsAspect(datasetUrn, paths),
            createContainerAspect(datasetUrn, databaseUrn),
            createSchemaMetadataAspect(tableName),
            config.attachDomain() ? createDomainAspect(datasetUrn) : null
    ).filter(Objects::nonNull);
    return result;
  }

  HoodieSchema getAvroSchemaWithoutMetadataFields(HoodieTableMetaClient metaClient) {
    try {
      return new TableSchemaResolver(metaClient).getTableSchema(true);
    } catch (Exception e) {
      throw new HoodieSyncException("Failed to read avro schema", e);
    }
  }
}