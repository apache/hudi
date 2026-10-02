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
 * Error-table writer backed by a real {@link SparkRDDWriteClient} over a real Hudi table. Unlike
 * the stub writers elsewhere in the tree, its {@link #commit} releases the write statuses it is
 * given (and the upstream error events), which is what makes a later read of that RDD re-run the
 * write.
 */
class RddBackedErrorTableWriter extends BaseErrorTableWriter<ErrorEvent<HoodieRecord>> implements AutoCloseable {

  // Raw: a parameterized client would reject the raw-element JavaRDD<HoodieRecord>.
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
