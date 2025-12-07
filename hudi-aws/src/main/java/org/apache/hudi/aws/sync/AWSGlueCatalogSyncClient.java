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

package org.apache.hudi.aws.sync;

import org.apache.hudi.aws.credentials.HoodieAWSCredentialsProviderFactory;
import org.apache.hudi.aws.sync.util.GluePartitionFilterGenerator;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.CustomizedThreadFactory;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.MapUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.GlueCatalogSyncClientConfig;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.SchemaDifference;
import org.apache.hudi.sync.common.HoodieSyncClient;
import org.apache.hudi.sync.common.model.FieldSchema;
import org.apache.hudi.sync.common.model.Partition;

import lombok.Getter;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueAsyncClient;
import software.amazon.awssdk.services.glue.GlueAsyncClientBuilder;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchDeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchDeletePartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequestEntry;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionResponse;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.CreatePartitionIndexRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeletePartitionIndexRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionIndexesRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionIndexesResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.KeySchemaElement;
import software.amazon.awssdk.services.glue.model.PartitionIndex;
import software.amazon.awssdk.services.glue.model.PartitionIndexDescriptor;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.PartitionValueList;
import software.amazon.awssdk.services.glue.model.Segment;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.TagResourceRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.hudi.common.fs.FSUtils.s3aToS3;
import static org.apache.hudi.common.util.MapUtils.containsAll;
import static org.apache.hudi.common.util.MapUtils.isNullOrEmpty;
import static org.apache.hudi.config.GlueCatalogSyncClientConfig.ALL_PARTITIONS_READ_PARALLELISM;
import static org.apache.hudi.config.GlueCatalogSyncClientConfig.CHANGED_PARTITIONS_READ_PARALLELISM;
import static org.apache.hudi.config.GlueCatalogSyncClientConfig.GLUE_METADATA_FILE_LISTING;
import static org.apache.hudi.config.GlueCatalogSyncClientConfig.GLUE_SYNC_DATABASE_NAME;
import static org.apache.hudi.config.GlueCatalogSyncClientConfig.GLUE_SYNC_RESOURCE_TAGS;
import static org.apache.hudi.config.GlueCatalogSyncClientConfig.GLUE_SYNC_TABLE_NAME;
import static org.apache.hudi.config.GlueCatalogSyncClientConfig.META_SYNC_PARTITION_INDEX_FIELDS;
import static org.apache.hudi.config.GlueCatalogSyncClientConfig.META_SYNC_PARTITION_INDEX_FIELDS_ENABLE;
import static org.apache.hudi.config.GlueCatalogSyncClientConfig.PARTITION_CHANGE_PARALLELISM;
import static org.apache.hudi.config.HoodieAWSConfig.AWS_GLUE_ENDPOINT;
import static org.apache.hudi.config.HoodieAWSConfig.AWS_GLUE_REGION;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_CREATE_MANAGED_TABLE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE;
import static org.apache.hudi.hive.util.HiveSchemaUtil.getPartitionKeyType;
import static org.apache.hudi.hive.util.HiveSchemaUtil.parquetSchemaToMapSchema;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.sync.common.util.TableUtils.tableId;

/**
 * This class implements all the AWS APIs to enable syncing of a Hudi Table with the
 * AWS Glue Data Catalog (https://docs.aws.amazon.com/glue/latest/dg/populate-data-catalog.html).
 *
 * @Experimental
 */
public class AWSGlueCatalogSyncClient extends HoodieSyncClient {

  private static final Logger LOG = LoggerFactory.getLogger(AWSGlueCatalogSyncClient.class);
  private static final int MAX_PARTITIONS_PER_CHANGE_REQUEST = 100;
  private static final int MAX_PARTITIONS_PER_READ_REQUEST = 1000;
  private static final int MAX_DELETE_PARTITIONS_PER_REQUEST = 25;
  protected final GlueAsyncClient awsGlue;
  private static final String GLUE_PARTITION_INDEX_ENABLE = "partition_filtering.enabled";
  private static final int PARTITION_INDEX_MAX_NUMBER = 3;
  /**
   * athena v2/v3 table property
   * see https://docs.aws.amazon.com/athena/latest/ug/querying-hudi.html
   */
  private static final String ENABLE_MDT_LISTING = "hudi.metadata-listing-enabled";
  private static final String GLUE_TABLE_ARN_FORMAT = "arn:aws:glue:%s:%s:table/%s/%s";
  private static final String GLUE_DATABASE_ARN_FORMAT = "arn:aws:glue:%s:%s:database/%s";
  @Getter
  private final String databaseName;
  @Getter
  private final String tableName;

  private final boolean skipTableArchive;
  private final String enableMetadataTable;
  private final int allPartitionsReadParallelism;
  private final int changedPartitionsReadParallelism;
  private final int changeParallelism;
  private final Map<String, Table> initialTableByName = new HashMap<>();
  private final String catalogId;

  public AWSGlueCatalogSyncClient(HiveSyncConfig config, HoodieTableMetaClient metaClient) {
    this(buildAsyncClient(config), StsClient.create(), config, metaClient);
  }

  AWSGlueCatalogSyncClient(GlueAsyncClient awsGlue, StsClient stsClient, HiveSyncConfig config, HoodieTableMetaClient metaClient) {
    super(config, metaClient);
    this.awsGlue = awsGlue;
    this.databaseName = config.getStringOrDefault(GLUE_SYNC_DATABASE_NAME, GLUE_SYNC_DATABASE_NAME.getInferFunction().get().apply(config).get());
    this.tableName = config.getStringOrDefault(GLUE_SYNC_TABLE_NAME, GLUE_SYNC_TABLE_NAME.getInferFunction().get().apply(config).get());
    this.skipTableArchive = config.getBooleanOrDefault(GlueCatalogSyncClientConfig.GLUE_SKIP_TABLE_ARCHIVE);
    this.enableMetadataTable = Boolean.toString(config.getBoolean(GLUE_METADATA_FILE_LISTING)).toUpperCase();
    this.allPartitionsReadParallelism = config.getIntOrDefault(ALL_PARTITIONS_READ_PARALLELISM);
    this.changedPartitionsReadParallelism = config.getIntOrDefault(CHANGED_PARTITIONS_READ_PARALLELISM);
    this.changeParallelism = config.getIntOrDefault(PARTITION_CHANGE_PARALLELISM);
    GetCallerIdentityResponse identityResponse = stsClient.getCallerIdentity(GetCallerIdentityRequest.builder().build());
    this.catalogId = config.getStringOrDefault(GlueCatalogSyncClientConfig.GLUE_CATALOG_ID, identityResponse.account());
  }

  private static GlueAsyncClient buildAsyncClient(HiveSyncConfig config) {
    try {
      GlueAsyncClientBuilder awsGlueBuilder = GlueAsyncClient.builder()
          .credentialsProvider(HoodieAWSCredentialsProviderFactory.getAwsCredentialsProvider(config.getProps()));
      awsGlueBuilder = config.getString(AWS_GLUE_ENDPOINT) == null ? awsGlueBuilder :
          awsGlueBuilder.endpointOverride(new URI(config.getString(AWS_GLUE_ENDPOINT)));
      awsGlueBuilder = config.getString(AWS_GLUE_REGION) == null ? awsGlueBuilder :
          awsGlueBuilder.region(Region.of(config.getString(AWS_GLUE_REGION)));
      return awsGlueBuilder.build();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private List<Partition> getPartitionsSegment(Segment segment, String tableName) {
    try {
      List<Partition> partitions = new ArrayList<>();
      String nextToken = null;
      do {
        GetPartitionsResponse result = awsGlue.getPartitions(GetPartitionsRequest.builder()
            .catalogId(catalogId)
            .databaseName(databaseName)
            .tableName(tableName)
            .excludeColumnSchema(true)
            .segment(segment)
            .nextToken(nextToken)
            .build()).get();
        partitions.addAll(result.partitions().stream()
            .map(p -> new Partition(p.values(), p.storageDescriptor().location()))
            .collect(Collectors.toList()));
        nextToken = result.nextToken();
      } while (nextToken != null);
      return partitions;
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Failed to get all partitions for table " + tableId(databaseName, tableName), e);
    }
  }

  private Table getInitialTable(String tableName) {
    return initialTableByName.computeIfAbsent(tableName, t -> getTable(awsGlue, databaseName, t));
  }

  @Override
  public List<Partition> getAllPartitions(String tableName) {
    ExecutorService executorService = Executors.newFixedThreadPool(this.allPartitionsReadParallelism, new CustomizedThreadFactory("glue-sync-all-partitions", true));
    try {
      List<Segment> segments = new ArrayList<>();
      for (int i = 0; i < allPartitionsReadParallelism; i++) {
        segments.add(Segment.builder()
            .segmentNumber(i)
            .totalSegments(allPartitionsReadParallelism).build());
      }
      List<Future<List<Partition>>> futures = segments.stream()
          .map(segment -> executorService.submit(() -> this.getPartitionsSegment(segment, tableName)))
          .collect(Collectors.toList());

      List<Partition> partitions = new ArrayList<>();
      for (Future<List<Partition>> future : futures) {
        partitions.addAll(future.get());
      }

      return partitions;
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Failed to get all partitions for table " + tableId(databaseName, tableName), e);
    } finally {
      executorService.shutdownNow();
    }
  }

  @Override
  public List<Partition> getPartitionsFromList(String tableName, List<String> partitionList) {
    if (partitionList.isEmpty()) {
      LOG.info("No partitions to read for " + tableId(this.databaseName, tableName));
      return Collections.emptyList();
    }
    HoodieTimer timer = HoodieTimer.start();
    List<List<String>> batches = CollectionUtils.batches(partitionList, MAX_PARTITIONS_PER_READ_REQUEST);
    ExecutorService executorService = Executors.newFixedThreadPool(
        Math.min(this.changedPartitionsReadParallelism, batches.size()),
        new CustomizedThreadFactory("glue-sync-get-partitions-" + tableName, true)
    );
    try {
      List<Future<List<Partition>>> futures = batches
          .stream()
          .map(batch -> executorService.submit(() -> this.getChangedPartitions(batch, tableName)))
          .collect(Collectors.toList());

      List<Partition> partitions = new ArrayList<>();
      for (Future<List<Partition>> future : futures) {
        partitions.addAll(future.get());
      }
      LOG.info(
          "Requested {} partitions, found existing {} partitions, new {} partitions",
          partitionList.size(),
          partitions.size(),
          partitionList.size() - partitions.size());

      return partitions;
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Failed to get all partitions for table " + tableId(this.databaseName, tableName), e);
    } finally {
      executorService.shutdownNow();
      LOG.info("Took {} ms to get {} partitions for table {}", timer.endTimer(), partitionList.size(), tableId(this.databaseName, tableName));
    }
  }

  private List<Partition> getChangedPartitions(List<String> changedPartitions, String tableName) throws ExecutionException, InterruptedException {
    List<PartitionValueList> partitionValueList = changedPartitions.stream().map(str ->
        PartitionValueList.builder().values(partitionValueExtractor.extractPartitionValuesInPath(str)).build()
    ).collect(Collectors.toList());
    BatchGetPartitionRequest request = BatchGetPartitionRequest.builder()
        .databaseName(this.databaseName)
        .tableName(tableName)
        .partitionsToGet(partitionValueList)
        .build();
    BatchGetPartitionResponse callResult = awsGlue.batchGetPartition(request).get();
    List<Partition> result = callResult
        .partitions()
        .stream()
        .map(p -> new Partition(p.values(), p.storageDescriptor().location()))
        .collect(Collectors.toList());

    return result;
  }

  @Override
  public void addPartitionsToTable(String tableName, List<String> partitionsToAdd) {
    HoodieTimer timer = HoodieTimer.start();
    try {
      if (partitionsToAdd.isEmpty()) {
        LOG.info("No partitions to add for " + tableId(this.databaseName, tableName));
        return;
      }
      Table table = getTable(awsGlue, databaseName, tableName);
      parallelizeChange(partitionsToAdd, this.changeParallelism, partitions -> this.addPartitionsToTableInternal(table, partitions), MAX_PARTITIONS_PER_CHANGE_REQUEST);
    } finally {
      LOG.info("Added {} partitions to table {} in {} ms", partitionsToAdd.size(), tableId(this.databaseName, tableName), timer.endTimer());
    }
  }

  private <T> void parallelizeChange(List<T> items, int parallelism, Consumer<List<T>> consumer, int sliceSize) {
    List<List<T>> batches = CollectionUtils.batches(items, sliceSize);
    ExecutorService executorService = Executors.newFixedThreadPool(Math.min(parallelism, batches.size()), new CustomizedThreadFactory("glue-sync", true));
    try {
      List<Future<?>> futures = batches.stream()
          .map(item -> executorService.submit(() -> {
            consumer.accept(item);
          }))
          .collect(Collectors.toList());
      for (Future<?> future : futures) {
        future.get();
      }
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Failed to parallelize operation", e);
    } finally {
      executorService.shutdownNow();
    }
  }

  private void addPartitionsToTableInternal(Table table, List<String> partitionsToAdd) {
    try {
      StorageDescriptor sd = table.storageDescriptor();
      List<PartitionInput> partitionInputList = partitionsToAdd.stream().map(partition -> {
        String fullPartitionPath = FSUtils.constructAbsolutePath(s3aToS3(getBasePath()), partition).toString();
        List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
        StorageDescriptor partitionSD = sd.copy(copySd -> copySd.location(fullPartitionPath));
        return PartitionInput.builder().values(partitionValues).storageDescriptor(partitionSD).build();
      }).collect(Collectors.toList());

      BatchCreatePartitionRequest request = BatchCreatePartitionRequest.builder().catalogId(catalogId)
          .databaseName(databaseName).tableName(table.name()).partitionInputList(partitionInputList).build();
      CompletableFuture<BatchCreatePartitionResponse> future = awsGlue.batchCreatePartition(request);
      BatchCreatePartitionResponse response = future.get();
      if (CollectionUtils.nonEmpty(response.errors())) {
        if (response.errors().stream()
            .allMatch(
                (error) -> "AlreadyExistsException".equals(error.errorDetail().errorCode()))) {
          LOG.info("Partitions already exist in glue: {}", response.errors());
        } else {
          throw new HoodieGlueSyncException("Fail to add partitions to " + tableId(databaseName, table.name())
              + " with error(s): " + response.errors());
        }
      }
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to add partitions to " + tableId(databaseName, table.name()), e);
    }
  }

  @Override
  public void updatePartitionsToTable(String tableName, List<String> changedPartitions) {
    HoodieTimer timer = HoodieTimer.start();
    try {
      if (changedPartitions.isEmpty()) {
        LOG.info("No partitions to update for " + tableId(this.databaseName, tableName));
        return;
      }
      Table table = getTable(awsGlue, databaseName, tableName);
      parallelizeChange(changedPartitions, this.changeParallelism, partitions -> this.updatePartitionsToTableInternal(table, partitions), MAX_PARTITIONS_PER_CHANGE_REQUEST);
    } finally {
      LOG.info("Updated {} partitions to table {} in {} ms", changedPartitions.size(), tableId(this.databaseName, tableName), timer.endTimer());
    }
  }

  private void updatePartitionsToTableInternal(Table table, List<String> changedPartitions) {
    try {
      StorageDescriptor sd = table.storageDescriptor();
      List<BatchUpdatePartitionRequestEntry> updatePartitionEntries = changedPartitions.stream().map(partition -> {
        String fullPartitionPath = FSUtils.constructAbsolutePath(s3aToS3(getBasePath()), partition).toString();
        List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
        StorageDescriptor partitionSD = sd.copy(copySd -> copySd.location(fullPartitionPath));
        PartitionInput partitionInput = PartitionInput.builder().values(partitionValues).storageDescriptor(partitionSD).build();
        return BatchUpdatePartitionRequestEntry.builder().partitionInput(partitionInput).partitionValueList(partitionValues).build();
      }).collect(Collectors.toList());

      BatchUpdatePartitionRequest request = BatchUpdatePartitionRequest.builder().catalogId(catalogId)
              .databaseName(databaseName).tableName(table.name()).entries(updatePartitionEntries).build();
      CompletableFuture<BatchUpdatePartitionResponse> future = awsGlue.batchUpdatePartition(request);

      BatchUpdatePartitionResponse response = future.get();
      if (CollectionUtils.nonEmpty(response.errors())) {
        throw new HoodieGlueSyncException("Fail to update partitions to " + tableId(databaseName, table.name())
            + " with error(s): " + response.errors());
      }
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to update partitions to " + tableId(databaseName, table.name()), e);
    }
  }

  @Override
  public void dropPartitions(String tableName, List<String> partitionsToDrop) {
    HoodieTimer timer = HoodieTimer.start();
    try {
      if (partitionsToDrop.isEmpty()) {
        LOG.info("No partitions to drop for " + tableId(this.databaseName, tableName));
        return;
      }
      parallelizeChange(partitionsToDrop, this.changeParallelism, partitions -> this.dropPartitionsInternal(tableName, partitions), MAX_DELETE_PARTITIONS_PER_REQUEST);
    } finally {
      LOG.info("Deleted {} partitions to table {} in {} ms", partitionsToDrop.size(), tableId(this.databaseName, tableName), timer.endTimer());
    }
  }

  private void dropPartitionsInternal(String tableName, List<String> partitionsToDrop) {
    try {
      List<PartitionValueList> partitionValueLists = partitionsToDrop.stream().map(partition -> PartitionValueList.builder()
            .values(partitionValueExtractor.extractPartitionValuesInPath(partition))
            .build()
      ).collect(Collectors.toList());

      BatchDeletePartitionRequest batchDeletePartitionRequest = BatchDeletePartitionRequest.builder()
            .catalogId(catalogId)
            .databaseName(databaseName)
            .tableName(tableName)
            .partitionsToDelete(partitionValueLists)
            .build();
      CompletableFuture<BatchDeletePartitionResponse> future = awsGlue.batchDeletePartition(batchDeletePartitionRequest);

      BatchDeletePartitionResponse response = future.get();
      if (CollectionUtils.nonEmpty(response.errors())) {
        throw new HoodieGlueSyncException("Fail to drop partitions to " + tableId(databaseName, tableName)
            + " with error(s): " + response.errors());
      }
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to drop partitions to " + tableId(databaseName, tableName), e);
    }
  }

  /**
   * Update the table properties to the table.
   */
  @Override
  public boolean updateTableProperties(String tableName, Map<String, String> tableProperties) {
    try {
      tableProperties.put(ENABLE_MDT_LISTING, enableMetadataTable);
      return updateTableParameters(awsGlue, databaseName, tableName, tableProperties, skipTableArchive);
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to update properties for table " + tableId(databaseName, tableName), e);
    }
  }

  private void setComments(List<Column> columns, Map<String, Option<String>> commentsMap) {
    columns.forEach(column -> {
      String comment = commentsMap.getOrDefault(column.name(), Option.empty()).orElse(null);
      Column.builder().comment(comment).build();
    });
  }

  private String getTableDoc() {
    try {
      return tableSchemaResolver.getTableAvroSchema(true).getDoc();
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Failed to get schema's doc from storage : ", e);
    }
  }

  @Override
  public List<FieldSchema> getStorageFieldSchemas() {
    try {
      return tableSchemaResolver.getTableAvroSchema(true)
          .getFields()
          .stream()
          .map(f -> new FieldSchema(f.name(), f.schema().getType().getName(), f.doc()))
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Failed to get field schemas from storage : ", e);
    }
  }

  @Override
  public boolean updateTableComments(String tableName, List<FieldSchema> fromMetastore, List<FieldSchema> fromStorage) {
    Table table = getTable(awsGlue, databaseName, tableName);

    Map<String, Option<String>> commentsMap = fromStorage.stream().collect(Collectors.toMap(FieldSchema::getName, FieldSchema::getComment));

    StorageDescriptor storageDescriptor = table.storageDescriptor();
    List<Column> columns = storageDescriptor.columns();
    setComments(columns, commentsMap);

    List<Column> partitionKeys = table.partitionKeys();
    setComments(partitionKeys, commentsMap);

    String tableDescription = getTableDoc();

    if (getTable(awsGlue, databaseName, tableName).storageDescriptor().equals(storageDescriptor)
        && getTable(awsGlue, databaseName, tableName).partitionKeys().equals(partitionKeys)) {
      // no comments have been modified / added
      return false;
    } else {
      final Instant now = Instant.now();
      TableInput updatedTableInput = TableInput.builder()
          .name(tableName)
          .description(tableDescription)
          .tableType(table.tableType())
          .parameters(table.parameters())
          .partitionKeys(partitionKeys)
          .storageDescriptor(storageDescriptor)
          .lastAccessTime(now)
          .lastAnalyzedTime(now)
          .build();

      UpdateTableRequest request = UpdateTableRequest.builder()
          .catalogId(catalogId)
          .databaseName(databaseName)
          .tableInput(updatedTableInput)
          .build();

      try {
        awsGlue.updateTable(request).get();
      } catch (Exception e) {
        throw new HoodieGlueSyncException("Fail to update table comments " + tableId(databaseName, table.name()), e);
      }
      return true;
    }
  }

  @Override
  public void updateTableSchema(String tableName, MessageType newSchema, SchemaDifference schemaDiff) {
    try {
      Table table = getTable(awsGlue, databaseName, tableName);
      Map<String, String> newSchemaMap = parquetSchemaToMapSchema(newSchema, config.getBoolean(HIVE_SUPPORT_TIMESTAMP_TYPE), false);
      List<Column> newColumns = getColumnsFromSchema(newSchemaMap);
      StorageDescriptor sd = table.storageDescriptor();
      StorageDescriptor partitionSD = sd.copy(copySd -> copySd.columns(newColumns));
      final Instant now = Instant.now();
      TableInput updatedTableInput = TableInput.builder()
          .name(tableName)
          .tableType(table.tableType())
          .parameters(table.parameters())
          .partitionKeys(table.partitionKeys())
          .storageDescriptor(partitionSD)
          .lastAccessTime(now)
          .lastAnalyzedTime(now)
          .build();

      UpdateTableRequest request = UpdateTableRequest.builder()
          .catalogId(catalogId)
          .databaseName(databaseName)
          .skipArchive(skipTableArchive)
          .tableInput(updatedTableInput)
          .build();

      awsGlue.updateTable(request).get();
      // glue needs partition schema cascading only when columns get updated
      // TODO: skip cascading when new fields in structs are added to the schema in last position
      boolean cascade = config.getSplitStrings(META_SYNC_PARTITION_FIELDS).size() > 0 && !schemaDiff.getUpdateColumnTypes().isEmpty();
      if (cascade) {
        LOG.info("Cascading column changes to partitions");
        List<String> allPartitions = getAllPartitions(tableName).stream()
            .map(partition -> getStringFromPartition(table.partitionKeys(), partition.getValues()))
            .collect(Collectors.toList());
        updatePartitionsToTable(tableName, allPartitions);
      }
      awsGlue.updateTable(request).get();
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to update definition for table " + tableId(databaseName, tableName), e);
    }
  }

  private String getStringFromPartition(List<Column> partitionKeys, List<String> values) {
    ArrayList<String> partitionValues = new ArrayList<>();
    for (int i = 0; i < partitionKeys.size(); i++) {
      partitionValues.add(String.format("%s=%s", partitionKeys.get(i).name(), values.get(i)));
    }
    return partitionValues.stream().collect(Collectors.joining("/"));
  }

  @Override
  public void createOrReplaceTable(String tableName,
                                   MessageType storageSchema,
                                   String inputFormatClass,
                                   String outputFormatClass,
                                   String serdeClass,
                                   Map<String, String> serdeProperties,
                                   Map<String, String> tableProperties) {

    if (!tableExists(tableName)) {
      // if table doesn't exist before, directly create new table.
      createTable(tableName, storageSchema, inputFormatClass, outputFormatClass, serdeClass, serdeProperties, tableProperties);
      return;
    }

    try {
      // validate before dropping the table
      validateSchemaAndProperties(tableName, storageSchema, inputFormatClass, outputFormatClass, serdeClass, serdeProperties, tableProperties);
      // drop and recreate the actual table
      dropTable(tableName);
      createTable(tableName, storageSchema, inputFormatClass, outputFormatClass, serdeClass, serdeProperties, tableProperties);
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to recreate the table" + tableId(databaseName, tableName), e);
    }
  }

  /**
   * creates a temp table with the given schema and properties to ensure
   * table creation succeeds before dropping the table and recreating it.
   * This ensures that actual table is not dropped in case there are any
   * issues with table creation because of provided schema or properties
   */
  private void validateSchemaAndProperties(String tableName,
                                           MessageType storageSchema,
                                           String inputFormatClass,
                                           String outputFormatClass,
                                           String serdeClass,
                                           Map<String, String> serdeProperties,
                                           Map<String, String> tableProperties) {
    // Create a temp table to validate the schema and properties
    String tempTableName = generateTempTableName(tableName);
    createTable(tempTableName, storageSchema, inputFormatClass, outputFormatClass, serdeClass, serdeProperties, tableProperties);
    // drop the temp table
    dropTable(tempTableName);
  }

  @Override
  public void createTable(String tableName,
                          MessageType storageSchema,
                          String inputFormatClass,
                          String outputFormatClass,
                          String serdeClass,
                          Map<String, String> serdeProperties,
                          Map<String, String> tableProperties) {
    if (tableExists(tableName)) {
      return;
    }
    Map<String, String> params = new HashMap<>();
    if (!config.getBoolean(HIVE_CREATE_MANAGED_TABLE)) {
      params.put("EXTERNAL", "TRUE");
    }
    params.put(ENABLE_MDT_LISTING, this.enableMetadataTable);
    params.putAll(tableProperties);

    try {
      Map<String, String> mapSchema = parquetSchemaToMapSchema(storageSchema, config.getBoolean(HIVE_SUPPORT_TIMESTAMP_TYPE), false);

      List<Column> schemaWithoutPartitionKeys = getColumnsFromSchema(mapSchema);

      // now create the schema partition
      List<Column> schemaPartitionKeys = config.getSplitStrings(META_SYNC_PARTITION_FIELDS).stream().map(partitionKey -> {
        String keyType = getPartitionKeyType(mapSchema, partitionKey);
        return Column.builder().name(partitionKey).type(keyType.toLowerCase()).comment("").build();
      }).collect(Collectors.toList());

      serdeProperties.put("serialization.format", "1");
      StorageDescriptor storageDescriptor = StorageDescriptor.builder()
          .serdeInfo(SerDeInfo.builder().serializationLibrary(serdeClass).parameters(serdeProperties).build())
          .location(s3aToS3(getBasePath()))
          .inputFormat(inputFormatClass)
          .outputFormat(outputFormatClass)
          .columns(schemaWithoutPartitionKeys)
          .build();

      final Instant now = Instant.now();
      TableInput tableInput = TableInput.builder()
          .name(tableName)
          .tableType(TableType.EXTERNAL_TABLE.toString())
          .parameters(params)
          .partitionKeys(schemaPartitionKeys)
          .storageDescriptor(storageDescriptor)
          .lastAccessTime(now)
          .lastAnalyzedTime(now)
          .build();

      CreateTableRequest request = CreateTableRequest.builder()
              .catalogId(catalogId)
              .databaseName(databaseName)
              .tableInput(tableInput)
              .build();

      CreateTableResponse response = awsGlue.createTable(request).get();
      LOG.info("Created table {} : {}", tableId(databaseName, tableName), response);
    } catch (AlreadyExistsException e) {
      LOG.warn("Table {} already exists.", tableId(databaseName, tableName), e);
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to create " + tableId(databaseName, tableName), e);
    }
  }

  /**
   * This will manage partitions indexes. Users can activate/deactivate them on existing tables.
   * Removing index definition, will result in dropping the index.
   * <p>
   * reference doc for partition indexes:
   * https://docs.aws.amazon.com/glue/latest/dg/partition-indexes.html#partition-index-getpartitions
   *
   * @param tableName
   */
  public void managePartitionIndexes(String tableName) throws ExecutionException, InterruptedException {
    if (!config.getBooleanOrDefault(META_SYNC_PARTITION_INDEX_FIELDS_ENABLE)) {
      // deactivate indexing if enabled
      if (getPartitionIndexEnable(tableName)) {
        LOG.info("Deactivating partition indexing");
        updatePartitionIndexEnable(tableName, false);
      }
      // also drop all existing indexes
      GetPartitionIndexesRequest indexesRequest = GetPartitionIndexesRequest.builder().databaseName(databaseName).tableName(tableName).build();
      GetPartitionIndexesResponse existingIdxsResp = awsGlue.getPartitionIndexes(indexesRequest).get();
      for (PartitionIndexDescriptor idsToDelete : existingIdxsResp.partitionIndexDescriptorList()) {
        LOG.info("Dropping partition index: {}", idsToDelete.indexName());
        DeletePartitionIndexRequest idxToDelete = DeletePartitionIndexRequest.builder()
                .databaseName(databaseName).tableName(tableName).indexName(idsToDelete.indexName()).build();
        awsGlue.deletePartitionIndex(idxToDelete).get();
      }
    } else {
      // activate indexing usage if disabled
      if (!getPartitionIndexEnable(tableName)) {
        LOG.info("Activating partition indexing");
        updatePartitionIndexEnable(tableName, true);
      }

      // get indexes to be created
      List<List<String>> partitionsIndexNeeded = parsePartitionsIndexConfig();
      // get existing indexes
      GetPartitionIndexesRequest indexesRequest = GetPartitionIndexesRequest.builder()
          .databaseName(databaseName).tableName(tableName).build();
      GetPartitionIndexesResponse existingIdxsResp = awsGlue.getPartitionIndexes(indexesRequest).get();

      // for each existing index remove if not relevant anymore
      boolean indexesChanges = false;
      for (PartitionIndexDescriptor existingIdx: existingIdxsResp.partitionIndexDescriptorList()) {
        List<String> idxColumns = existingIdx.keys().stream().map(KeySchemaElement::name).collect(Collectors.toList());
        boolean toBeRemoved = true;
        for (List<String> neededIdx : partitionsIndexNeeded) {
          if (neededIdx.equals(idxColumns)) {
            toBeRemoved = false;
          }
        }
        if (toBeRemoved) {
          indexesChanges = true;
          DeletePartitionIndexRequest idxToDelete = DeletePartitionIndexRequest.builder()
                  .databaseName(databaseName).tableName(tableName).indexName(existingIdx.indexName()).build();
          LOG.info("Dropping irrelevant index: {}", existingIdx.indexName());
          awsGlue.deletePartitionIndex(idxToDelete).get();
        }
      }
      if (indexesChanges) { // refresh indexes list
        existingIdxsResp = awsGlue.getPartitionIndexes(indexesRequest).get();
      }

      // for each needed index create if not exist
      for (List<String> neededIdx : partitionsIndexNeeded) {
        Boolean toBeCreated = true;
        for (PartitionIndexDescriptor existingIdx: existingIdxsResp.partitionIndexDescriptorList()) {
          List<String> collect = existingIdx.keys().stream().map(key -> key.name()).collect(Collectors.toList());
          if (collect.equals(neededIdx)) {
            toBeCreated = false;
          }
        }
        if (toBeCreated) {
          String newIdxName = String.format("hudi_managed_%s", neededIdx.toString());
          PartitionIndex newIdx = PartitionIndex.builder()
                  .indexName(newIdxName)
                  .keys(neededIdx).build();
          LOG.info("Creating new partition index: {}", newIdxName);
          CreatePartitionIndexRequest creationRequest = CreatePartitionIndexRequest.builder()
                  .databaseName(databaseName).tableName(tableName).partitionIndex(newIdx).build();
          awsGlue.createPartitionIndex(creationRequest).get();
        }
      }
    }
  }

  protected List<List<String>> parsePartitionsIndexConfig() {
    config.setDefaultValue(META_SYNC_PARTITION_INDEX_FIELDS);
    String rawPartitionIndex = config.getString(META_SYNC_PARTITION_INDEX_FIELDS);
    List<List<String>> indexes = Arrays.stream(rawPartitionIndex.split(","))
                                       .map(idx -> Arrays.stream(idx.split(";"))
                                                         .collect(Collectors.toList())).collect(Collectors.toList());
    if (indexes.size() > PARTITION_INDEX_MAX_NUMBER) {
      LOG.warn("Only considering first {} indexes", PARTITION_INDEX_MAX_NUMBER);
      return indexes.subList(0, PARTITION_INDEX_MAX_NUMBER);
    }
    return indexes;
  }

  public Boolean getPartitionIndexEnable(String tableName) {
    try {
      Table table = getTable(awsGlue, databaseName, tableName);
      return Boolean.valueOf(table.parameters().get(GLUE_PARTITION_INDEX_ENABLE));
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to get parameter " + GLUE_PARTITION_INDEX_ENABLE + " time for " + tableId(databaseName, tableName), e);
    }
  }

  public void updatePartitionIndexEnable(String tableName, Boolean enable) {
    try {
      updateTableParameters(awsGlue, databaseName, tableName, Collections.singletonMap(GLUE_PARTITION_INDEX_ENABLE, enable.toString()), false);
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to update parameter " + GLUE_PARTITION_INDEX_ENABLE + " time for " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public Map<String, String> getMetastoreSchema(String tableName) {
    try {
      // GlueMetastoreClient returns partition keys separate from Columns, hence get both and merge to
      // get the Schema of the table.
      Table table = getInitialTable(tableName);
      Map<String, String> partitionKeysMap =
          table.partitionKeys().stream().collect(Collectors.toMap(Column::name, f -> f.type().toUpperCase()));

      Map<String, String> columnsMap =
          table.storageDescriptor().columns().stream().collect(Collectors.toMap(Column::name, f -> f.type().toUpperCase()));

      Map<String, String> schema = new HashMap<>();
      schema.putAll(columnsMap);
      schema.putAll(partitionKeysMap);
      return schema;
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to get schema for table " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public boolean tableExists(String tableName) {
    GetTableRequest request = GetTableRequest.builder()
        .catalogId(catalogId)
        .databaseName(databaseName)
        .name(tableName)
        .build();
    try {
      Table table = awsGlue.getTable(request).get().table();
      if (table != null) {
        initialTableByName.put(tableName, table);
      }
      return Objects.nonNull(table);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof EntityNotFoundException) {
        LOG.info("Table not found: {}.{}", databaseName, tableName, e);
        return false;
      } else {
        throw new HoodieGlueSyncException("Fail to get table: " + tableId(databaseName, tableName), e);
      }
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to get table: " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public boolean databaseExists(String databaseName) {
    GetDatabaseRequest request = GetDatabaseRequest.builder()
        .catalogId(catalogId)
        .name(databaseName)
        .build();
    try {
      return Objects.nonNull(awsGlue.getDatabase(request).get().database());
    } catch (ExecutionException e) {
      if (e.getCause() instanceof EntityNotFoundException) {
        LOG.info("Database not found: {}", databaseName, e);
        return false;
      } else {
        throw new HoodieGlueSyncException("Fail to check if database exists " + databaseName, e);
      }
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to check if database exists " + databaseName, e);
    }
  }

  @Override
  public void createDatabase(String databaseName) {
    if (databaseExists(databaseName)) {
      return;
    }
    CreateDatabaseRequest request = CreateDatabaseRequest.builder()
            .catalogId(catalogId)
            .databaseInput(DatabaseInput.builder()
            .name(databaseName)
            .description("Automatically created by " + this.getClass().getName())
            .parameters(null)
            .locationUri(null)
            .build()
    ).build();
    try {
      CreateDatabaseResponse result = awsGlue.createDatabase(request).get();
      tagResource(String.format(GLUE_DATABASE_ARN_FORMAT, awsGlue.serviceClientConfiguration().region(), catalogId, databaseName));
      LOG.info("Successfully created database in AWS Glue: {}", result.toString());
    } catch (AlreadyExistsException e) {
      LOG.info("AWS Glue Database {} already exists", databaseName, e);
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to create database " + databaseName, e);
    }
  }

  @Override
  public Option<String> getLastCommitTimeSynced(String tableName) {
    try {
      return Option.ofNullable(getInitialTable(tableName).parameters().getOrDefault(HOODIE_LAST_COMMIT_TIME_SYNC, null));
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to get last sync commit time for " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public Option<String> getLastCommitCompletionTimeSynced(String tableName) {
    // Get the last commit completion time from the TBLproperties
    try {
      return Option.ofNullable(getInitialTable(tableName).parameters().getOrDefault(HOODIE_LAST_COMMIT_COMPLETION_TIME_SYNC, null));
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Failed to get the last commit completion time synced from the table " + tableName, e);
    }
  }

  @Override
  public void close() {
    awsGlue.close();
  }

  @Override
  public void updateLastCommitTimeSynced(String tableName) {
    HoodieTimeline activeTimeline = getActiveTimeline();
    Option<String> lastCommitSynced = activeTimeline.lastInstant().map(HoodieInstant::requestedTime);
    Option<String> lastCommitCompletionSynced = activeTimeline
        .getLatestCompletionTime();
    if (lastCommitSynced.isPresent()) {
      try {
        HashMap<String, String> propertyMap = new HashMap<>();
        propertyMap.put(HOODIE_LAST_COMMIT_TIME_SYNC, lastCommitSynced.get());
        if (lastCommitCompletionSynced.isPresent()) {
          propertyMap.put(HOODIE_LAST_COMMIT_COMPLETION_TIME_SYNC, lastCommitCompletionSynced.get());
        }
        updateTableParameters(awsGlue, databaseName, tableName, propertyMap, skipTableArchive);
      } catch (Exception e) {
        throw new HoodieGlueSyncException("Fail to update last sync commit time for " + tableId(databaseName, tableName), e);
      }
    } else {
      LOG.info("No commit in active timeline.");
    }
    try {
      // as a side effect, we also refresh the partition indexes if needed
      // people may wan't to add indexes, without re-creating the table
      // therefore we call this at each commit as a workaround
      managePartitionIndexes(tableName);
    } catch (ExecutionException e) {
      LOG.warn("An indexation process is currently running.", e);
    } catch (Exception e) {
      LOG.warn("Something went wrong with partition index", e);
    }
  }

  @Override
  public void dropTable(String tableName) {
    DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder()
        .catalogId(catalogId)
        .databaseName(databaseName)
        .name(tableName)
        .build();

    try {
      awsGlue.deleteTable(deleteTableRequest).get();
      LOG.info("Successfully deleted table in AWS Glue: {}.{}", databaseName, tableName);
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        // In case {@code InterruptedException} was thrown, resetting the interrupted flag
        // of the thread, we reset it (to true) again to permit subsequent handlers
        // to be interrupted as well
        Thread.currentThread().interrupt();
      }
      throw new HoodieGlueSyncException("Failed to delete table " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public List<FieldSchema> getMetastoreFieldSchemas(String tableName) {
    try {
      Table table = getTable(awsGlue, databaseName, tableName);
      List<FieldSchema> partitionFields = getFieldSchemas(table.partitionKeys());
      List<FieldSchema> columnsFields = getFieldSchemas(table.storageDescriptor().columns());
      columnsFields.addAll(partitionFields);
      return columnsFields;
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Failed to get field schemas from metastore for table : " + tableId(databaseName, tableName), e);
    }
  }

  private List<FieldSchema> getFieldSchemas(List<Column> columns) {
    return columns.stream().map(column -> new FieldSchema(column.name(), column.type(), column.comment())).collect(Collectors.toList());
  }

  @Override
  public Option<String> getLastReplicatedTime(String tableName) {
    throw new UnsupportedOperationException("Not supported: `getLastReplicatedTime`");
  }

  @Override
  public void updateLastReplicatedTimeStamp(String tableName, String timeStamp) {
    throw new UnsupportedOperationException("Not supported: `updateLastReplicatedTimeStamp`");
  }

  @Override
  public void deleteLastReplicatedTimeStamp(String tableName) {
    throw new UnsupportedOperationException("Not supported: `deleteLastReplicatedTimeStamp`");
  }

  @Override
  public String generatePushDownFilter(List<String> writtenPartitions, List<FieldSchema> partitionFields) {
    return new GluePartitionFilterGenerator().generatePushDownFilter(writtenPartitions, partitionFields, (HiveSyncConfig) config);
  }

  @Override
  public boolean updateSerdeProperties(String tableName, Map<String, String> serdeProperties, boolean useRealtimeFormat) {
    if (MapUtils.isNullOrEmpty(serdeProperties)) {
      return false;
    }

    try {
      serdeProperties.putIfAbsent("serialization.format", "1");
      Table table = getTable(awsGlue, databaseName, tableName);
      StorageDescriptor existingTableStorageDescriptor = table.storageDescriptor();

      if (existingTableStorageDescriptor != null && existingTableStorageDescriptor.serdeInfo() != null
              && existingTableStorageDescriptor.serdeInfo().parameters().size() == serdeProperties.size()) {
        Map<String, String> existingSerdeProperties = existingTableStorageDescriptor.serdeInfo().parameters();
        boolean different = serdeProperties.entrySet().stream().anyMatch(e ->
                !existingSerdeProperties.containsKey(e.getKey()) || !existingSerdeProperties.get(e.getKey()).equals(e.getValue()));
        if (!different) {
          LOG.debug("Table {} serdeProperties already up to date, skip update serde properties.", tableName);
          return false;
        }
      }

      HoodieFileFormat baseFileFormat = HoodieFileFormat.valueOf(config.getStringOrDefault(META_SYNC_BASE_FILE_FORMAT).toUpperCase());
      String serDeClassName = HoodieInputFormatUtils.getSerDeClassName(baseFileFormat);

      SerDeInfo newSerdeInfo = SerDeInfo
          .builder()
          .serializationLibrary(serDeClassName)
          .parameters(serdeProperties)
          .build();

      StorageDescriptor storageDescriptor = table
          .storageDescriptor()
          .toBuilder()
          .serdeInfo(newSerdeInfo)
          .build();

      TableInput updatedTableInput = TableInput.builder()
          .name(tableName)
          .tableType(table.tableType())
          .parameters(table.parameters())
          .partitionKeys(table.partitionKeys())
          .storageDescriptor(storageDescriptor)
          .lastAccessTime(table.lastAccessTime())
          .lastAccessTime(table.lastAnalyzedTime())
          .build();

      UpdateTableRequest updateTableRequest = UpdateTableRequest.builder()
          .databaseName(databaseName)
          .tableInput(updatedTableInput)
          .build();

      awsGlue.updateTable(updateTableRequest).get();
      return true;
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Failed to update table serde info for table: "
              + tableName, e);
    }
  }

  @Override
  public String getTableLocation(String tableName) {
    try {
      return getInitialTable(tableName).storageDescriptor().location();
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to get base path for the table " + tableId(databaseName, tableName), e);
    }
  }

  private List<Column> getColumnsFromSchema(Map<String, String> mapSchema) {
    List<Column> cols = new ArrayList<>();
    for (String key : mapSchema.keySet()) {
      // In Glue, the full schema should exclude the partition keys
      if (!config.getSplitStrings(META_SYNC_PARTITION_FIELDS).contains(key)) {
        String keyType = getPartitionKeyType(mapSchema, key);
        Column column = Column.builder().name(key).type(keyType.toLowerCase()).comment("").build();
        cols.add(column);
      }
    }
    return cols;
  }

  private enum TableType {
    MANAGED_TABLE,
    EXTERNAL_TABLE,
    VIRTUAL_VIEW,
    INDEX_TABLE,
    MATERIALIZED_VIEW
  }

  private Table getTable(GlueAsyncClient awsGlue, String databaseName, String tableName) throws HoodieGlueSyncException {
    GetTableRequest request = GetTableRequest.builder()
        .catalogId(catalogId)
        .databaseName(databaseName)
        .name(tableName)
        .build();
    try {
      return awsGlue.getTable(request).get().table();
    } catch (EntityNotFoundException e) {
      throw new HoodieGlueSyncException("Table not found: " + tableId(databaseName, tableName), e);
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to get table " + tableId(databaseName, tableName), e);
    }
  }

  private boolean updateTableParameters(GlueAsyncClient awsGlue, String databaseName, String tableName, Map<String, String> updatingParams, boolean skipTableArchive) {
    if (isNullOrEmpty(updatingParams)) {
      return false;
    }
    try {
      Table table = getTable(awsGlue, databaseName, tableName);
      Map<String, String> remoteParams = table.parameters();
      if (containsAll(remoteParams, updatingParams)) {
        return false;
      }

      final Map<String, String> newParams = new HashMap<>();
      newParams.putAll(table.parameters());
      newParams.putAll(updatingParams);

      final Instant now = Instant.now();
      TableInput updatedTableInput = TableInput.builder()
          .name(tableName)
          .tableType(table.tableType())
          .parameters(newParams)
          .partitionKeys(table.partitionKeys())
          .storageDescriptor(table.storageDescriptor())
          .lastAccessTime(now)
          .lastAnalyzedTime(now)
          .build();

      UpdateTableRequest request =  UpdateTableRequest.builder().databaseName(databaseName)
          .catalogId(catalogId)
          .tableInput(updatedTableInput)
          .skipArchive(skipTableArchive)
          .build();
      awsGlue.updateTable(request).get();
      return true;
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to update params for table " + tableId(databaseName, tableName) + ": " + updatingParams, e);
    }
  }

  private void tagResource(String resourceArn) {
    Map<String, String> resourceTags = parseResourceTags(config.getStringOrDefault(GLUE_SYNC_RESOURCE_TAGS, ""));
    if (resourceTags.isEmpty()) {
      return;
    }
    TagResourceRequest tagRequest = TagResourceRequest.builder()
        .resourceArn(resourceArn)
        .tagsToAdd(resourceTags)
        .build();
    awsGlue.tagResource(tagRequest).join();
  }

  private Map<String, String> parseResourceTags(String resourceTagKeyValues) {
    Map<String, String> tags = new HashMap<>();
    if (resourceTagKeyValues == null || resourceTagKeyValues.trim().isEmpty()) {
      return tags;
    }
    String[] tagPairs = resourceTagKeyValues.split(",");
    for (String tagPair : tagPairs) {
      String[] keyValue = tagPair.split(":", 2);
      if (keyValue.length == 2) {
        tags.put(keyValue[0].trim(), keyValue[1].trim());
      }
    }
    return tags;
  }
}
