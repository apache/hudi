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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

/**
 * An error-table writer backed by a real {@link SparkRDDWriteClient} writing a real Hudi table,
 * mirroring the RDD lifecycle of the production writer used with
 * {@code hoodie.errortable.write.unification.enabled=true}:
 *
 * <ul>
 *   <li>{@link #upsert} persists the error events, starts an error-table instant and returns the
 *       <i>lazy</i> write-status RDD of a bulk insert. Nothing has been written yet.</li>
 *   <li>{@link #commit} hands those statuses to the write client. With
 *       {@code hoodie.release.resource.on.completion.enable=true} that commit calls
 *       {@code releaseResources}, which unpersists the write statuses cached under the instant's
 *       cache key, and the writer then drops its own cache of the upstream error events.</li>
 * </ul>
 *
 * <p>Both caches are therefore gone once {@link #commit} returns, so any later action on the
 * write-status RDD re-runs the bulk insert and lands every error record a second time under an
 * instant that is already complete. The stub error-table writers elsewhere in the test tree keep a
 * reference to the RDD and release nothing, which makes that ordering hazard invisible; this writer
 * exists so a test can observe it.
 *
 * @see TestErrorTableWriteOnce
 */
class RddBackedErrorTableWriter extends BaseErrorTableWriter<ErrorEvent<HoodieRecord>> implements AutoCloseable {

  // Raw on purpose: a parameterized SparkRDDWriteClient<T> would reject the raw-element
  // JavaRDD<HoodieRecord> that addErrorEvents accumulates.
  private final transient SparkRDDWriteClient writeClient;
  private transient JavaRDD<HoodieRecord> errorEventsRdd;
  private Option<String> errorTableInstantTime = Option.empty();

  RddBackedErrorTableWriter(SparkSession sparkSession,
                            HoodieSparkEngineContext context,
                            HoodieWriteConfig writeConfig) {
    super(new HoodieStreamer.Config(), sparkSession, new TypedProperties(), context, null);
    this.writeClient = new SparkRDDWriteClient(context, writeConfig);
  }

  @Override
  public void addErrorEvents(JavaRDD<ErrorEvent<HoodieRecord>> errorEvent) {
    JavaRDD<HoodieRecord> records = errorEvent.map(ErrorEvent::getPayload);
    errorEventsRdd = errorEventsRdd == null ? records : errorEventsRdd.union(records);
  }

  @Override
  public Option<JavaRDD<HoodieAvroIndexedRecord>> getErrorEvents(String baseTableInstantTime, Option<String> committedInstantTime) {
    throw new UnsupportedOperationException("getErrorEvents is not needed by the unified write path");
  }

  @Override
  public boolean upsertAndCommit(String baseTableInstantTime, Option<String> committedInstantTime) {
    return commit(upsert(baseTableInstantTime, committedInstantTime));
  }

  @Override
  public JavaRDD<WriteStatus> upsert(String baseTableInstantTime, Option<String> committedInstantTime) {
    ValidationUtils.checkArgument(errorEventsRdd != null,
        "addErrorEvents must be called before upsert");
    if (errorEventsRdd.getStorageLevel().equals(StorageLevel.NONE())) {
      errorEventsRdd.persist(StorageLevel.MEMORY_AND_DISK());
    }
    errorTableInstantTime = Option.of(writeClient.startCommit());
    return writeClient.bulkInsert(errorEventsRdd, errorTableInstantTime.get());
  }

  @Override
  public boolean commit(JavaRDD<WriteStatus> writeStatuses) {
    ValidationUtils.checkArgument(writeStatuses != null, "writeStatuses cannot be null");
    ValidationUtils.checkArgument(errorTableInstantTime.isPresent(),
        "Error table instant time is empty, which should be set in upsert method.");
    try {
      return writeClient.commit(errorTableInstantTime.get(), writeStatuses);
    } finally {
      if (errorEventsRdd != null && !errorEventsRdd.getStorageLevel().equals(StorageLevel.NONE())) {
        errorEventsRdd.unpersist();
      }
      errorEventsRdd = null;
    }
  }

  @Override
  public void close() {
    writeClient.close();
  }
}
