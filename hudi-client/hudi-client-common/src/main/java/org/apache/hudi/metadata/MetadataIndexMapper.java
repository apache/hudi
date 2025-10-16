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

package org.apache.hudi.metadata;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * Abstract base class for metadata index mappers. Each index type can extend this to implement
 * its own record generation logic.
 */
public abstract class MetadataIndexMapper implements SerializableFunction<WriteStatus, Iterator<HoodieRecord>>, Serializable {
  protected final HoodieWriteConfig dataWriteConfig;

  public MetadataIndexMapper(HoodieWriteConfig dataWriteConfig) {
    this.dataWriteConfig = dataWriteConfig;
  }

  /**
   * Generates metadata index records from a WriteStatus.
   *
   * @param writeStatus the write status to process
   * @return list of metadata records
   */
  protected abstract List<HoodieRecord> generateRecords(WriteStatus writeStatus);

  /**
   * Post-processes the generated records. Default implementation returns records as-is.
   * Subclasses can override to add deduplication, validation, or other transformations.
   *
   * @param records the generated records
   * @return post-processed records
   */
  public HoodieData<HoodieRecord> postProcess(HoodieData<HoodieRecord> records) {
    return records;
  }

  @Override
  public final Iterator<HoodieRecord> apply(WriteStatus writeStatus) throws Exception {
    return generateRecords(writeStatus).iterator();
  }
}
