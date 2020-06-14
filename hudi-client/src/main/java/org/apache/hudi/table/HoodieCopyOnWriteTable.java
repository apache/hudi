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

package org.apache.hudi.table;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.utils.ParquetReaderIterator;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.execution.SparkBoundedInMemoryExecutor;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.clean.CleanActionExecutor;
import org.apache.hudi.table.action.commit.BulkInsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.BulkInsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.DeleteCommitActionExecutor;
import org.apache.hudi.table.action.commit.InsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.InsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.UpsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.UpsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.restore.CopyOnWriteRestoreActionExecutor;
import org.apache.hudi.table.action.rollback.CopyOnWriteRollbackActionExecutor;
import org.apache.hudi.table.action.savepoint.SavepointActionExecutor;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Implementation of a very heavily read-optimized Hoodie Table where, all data is stored in base files, with
 * zero read amplification.
 *
 * <p>
 * INSERTS - Produce new files, block aligned to desired size (or) Merge with the smallest existing file, to expand it
 * <p>
 * UPDATES - Produce a new version of the file, just replacing the updated records with new values
 */
public class HoodieCopyOnWriteTable<T extends HoodieRecordPayload> extends HoodieTable<T> {

  private static final Logger LOG = LogManager.getLogger(HoodieCopyOnWriteTable.class);

  public HoodieCopyOnWriteTable(HoodieWriteConfig config, Configuration hadoopConf, HoodieTableMetaClient metaClient) {
    super(config, hadoopConf, metaClient);
  }

  @Override
  public HoodieWriteMetadata upsert(JavaSparkContext jsc, String instantTime, JavaRDD<HoodieRecord<T>> records) {
    return new UpsertCommitActionExecutor<>(jsc, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata insert(JavaSparkContext jsc, String instantTime, JavaRDD<HoodieRecord<T>> records) {
    return new InsertCommitActionExecutor<>(jsc, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata bulkInsert(JavaSparkContext jsc, String instantTime, JavaRDD<HoodieRecord<T>> records,
      Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    return new BulkInsertCommitActionExecutor<>(jsc, config,
        this, instantTime, records, bulkInsertPartitioner).execute();
  }

  @Override
  public HoodieWriteMetadata delete(JavaSparkContext jsc, String instantTime, JavaRDD<HoodieKey> keys) {
    return new DeleteCommitActionExecutor<>(jsc, config, this, instantTime, keys).execute();
  }

  @Override
  public HoodieWriteMetadata upsertPrepped(JavaSparkContext jsc, String instantTime,
      JavaRDD<HoodieRecord<T>> preppedRecords) {
    return new UpsertPreppedCommitActionExecutor<>(jsc, config, this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata insertPrepped(JavaSparkContext jsc, String instantTime,
      JavaRDD<HoodieRecord<T>> preppedRecords) {
    return new InsertPreppedCommitActionExecutor<>(jsc, config, this, instantTime, preppedRecords).execute();
  }

  @Override
  public HoodieWriteMetadata bulkInsertPrepped(JavaSparkContext jsc, String instantTime,
      JavaRDD<HoodieRecord<T>> preppedRecords,  Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    return new BulkInsertPreppedCommitActionExecutor<>(jsc, config,
        this, instantTime, preppedRecords, bulkInsertPartitioner).execute();
  }

  @Override
  public Option<HoodieCompactionPlan> scheduleCompaction(JavaSparkContext jsc, String instantTime, Option<Map<String, String>> extraMetadata) {
    throw new HoodieNotSupportedException("Compaction is not supported on a CopyOnWrite table");
  }

  @Override
  public HoodieWriteMetadata compact(JavaSparkContext jsc, String compactionInstantTime) {
    throw new HoodieNotSupportedException("Compaction is not supported on a CopyOnWrite table");
  }

  public Iterator<List<WriteStatus>> handleUpdate(String instantTime, String partitionPath, String fileId,
      Map<String, HoodieRecord<T>> keyToNewRecords, HoodieBaseFile oldDataFile) throws IOException {
    // these are updates
    HoodieMergeHandle upsertHandle = getUpdateHandle(instantTime, partitionPath, fileId, keyToNewRecords, oldDataFile);
    return handleUpdateInternal(upsertHandle, instantTime, fileId);
  }

  protected Iterator<List<WriteStatus>> handleUpdateInternal(HoodieMergeHandle upsertHandle, String instantTime,
      String fileId) throws IOException {
    if (upsertHandle.getOldFilePath() == null) {
      throw new HoodieUpsertException(
          "Error in finding the old file path at commit " + instantTime + " for fileId: " + fileId);
    } else {
      AvroReadSupport.setAvroReadSchema(getHadoopConf(), upsertHandle.getWriterSchema());
      BoundedInMemoryExecutor<GenericRecord, GenericRecord, Void> wrapper = null;
      try (ParquetReader<IndexedRecord> reader =
          AvroParquetReader.<IndexedRecord>builder(upsertHandle.getOldFilePath()).withConf(getHadoopConf()).build()) {
        wrapper = new SparkBoundedInMemoryExecutor(config, new ParquetReaderIterator(reader),
            new UpdateHandler(upsertHandle), x -> x);
        wrapper.execute();
      } catch (Exception e) {
        throw new HoodieException(e);
      } finally {
        upsertHandle.close();
        if (null != wrapper) {
          wrapper.shutdownNow();
        }
      }
    }

    // TODO(vc): This needs to be revisited
    if (upsertHandle.getWriteStatus().getPartitionPath() == null) {
      LOG.info("Upsert Handle has partition path as null " + upsertHandle.getOldFilePath() + ", "
          + upsertHandle.getWriteStatus());
    }
    return Collections.singletonList(Collections.singletonList(upsertHandle.getWriteStatus())).iterator();
  }

  protected HoodieMergeHandle getUpdateHandle(String instantTime, String partitionPath, String fileId,
      Map<String, HoodieRecord<T>> keyToNewRecords, HoodieBaseFile dataFileToBeMerged) {
    return new HoodieMergeHandle<>(config, instantTime, this, keyToNewRecords,
            partitionPath, fileId, dataFileToBeMerged, sparkTaskContextSupplier);
  }

  public Iterator<List<WriteStatus>> handleInsert(String instantTime, String partitionPath, String fileId,
      Iterator<HoodieRecord<T>> recordItr) {
    HoodieCreateHandle createHandle =
        new HoodieCreateHandle(config, instantTime, this, partitionPath, fileId, recordItr, sparkTaskContextSupplier);
    createHandle.write();
    return Collections.singletonList(Collections.singletonList(createHandle.close())).iterator();
  }

  @Override
  public HoodieCleanMetadata clean(JavaSparkContext jsc, String cleanInstantTime) {
    return new CleanActionExecutor(jsc, config, this, cleanInstantTime).execute();
  }

  @Override
  public HoodieRollbackMetadata rollback(JavaSparkContext jsc, String rollbackInstantTime, HoodieInstant commitInstant, boolean deleteInstants) {
    return new CopyOnWriteRollbackActionExecutor(jsc, config, this, rollbackInstantTime, commitInstant, deleteInstants).execute();
  }

  @Override
  public HoodieSavepointMetadata savepoint(JavaSparkContext jsc, String instantToSavepoint, String user, String comment) {
    return new SavepointActionExecutor(jsc, config, this, instantToSavepoint, user, comment).execute();
  }

  @Override
  public HoodieRestoreMetadata restore(JavaSparkContext jsc, String restoreInstantTime, String instantToRestore) {
    return new CopyOnWriteRestoreActionExecutor(jsc, config, this, restoreInstantTime, instantToRestore).execute();
  }

  enum BucketType {
    UPDATE, INSERT
  }

  /**
   * Consumer that dequeues records from queue and sends to Merge Handle.
   */
  private static class UpdateHandler extends BoundedInMemoryQueueConsumer<GenericRecord, Void> {

    private final HoodieMergeHandle upsertHandle;

    private UpdateHandler(HoodieMergeHandle upsertHandle) {
      this.upsertHandle = upsertHandle;
    }

    @Override
    protected void consumeOneRecord(GenericRecord record) {
      upsertHandle.write(record);
    }

    @Override
    protected void finish() {}

    @Override
    protected Void getResult() {
      return null;
    }
  }

  /**
   * Helper class for a small file's location and its actual size on disk.
   */
  static class SmallFile implements Serializable {

    HoodieRecordLocation location;
    long sizeBytes;

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("SmallFile {");
      sb.append("location=").append(location).append(", ");
      sb.append("sizeBytes=").append(sizeBytes);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Helper class for an insert bucket along with the weight [0.0, 1.0] that defines the amount of incoming inserts that
   * should be allocated to the bucket.
   */
  class InsertBucket implements Serializable {

    int bucketNumber;
    // fraction of total inserts, that should go into this bucket
    double weight;

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("WorkloadStat {");
      sb.append("bucketNumber=").append(bucketNumber).append(", ");
      sb.append("weight=").append(weight);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Helper class for a bucket's type (INSERT and UPDATE) and its file location.
   */
  class BucketInfo implements Serializable {

    BucketType bucketType;
    String fileIdPrefix;
    String partitionPath;

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("BucketInfo {");
      sb.append("bucketType=").append(bucketType).append(", ");
      sb.append("fileIdPrefix=").append(fileIdPrefix).append(", ");
      sb.append("partitionPath=").append(partitionPath);
      sb.append('}');
      return sb.toString();
    }
  }
}
