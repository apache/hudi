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

package org.apache.hudi.index.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.common.util.DateTimeUtils.parseDuration;

/**
 * Hoodie Redis Index implementation.
 */
public class HoodieRedisIndex
    extends HoodieIndex<Object, Object> {

  private static final Logger LOG = LogManager.getLogger(HoodieRedisIndex.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final HoodieWriteConfig configuration;

  private final Duration expireTime;

  private transient RedisClient client;

  public HoodieRedisIndex(HoodieWriteConfig configuration) {
    super(configuration);
    this.configuration = configuration;
    client = RedisClientFactory.getClient(configuration);
    expireTime = parseDuration(configuration.getRedisIndexExpireTime());
  }

  @Override
  public <R> HoodieData<HoodieRecord<R>> tagLocation(
      HoodieData<HoodieRecord<R>> records, HoodieEngineContext context,
      HoodieTable hoodieTable) {
    return records.mapPartitions(hoodieRecordIterator -> {
      List<HoodieRecord<R>> taggedRecords = new ArrayList<>();
      while (hoodieRecordIterator.hasNext()) {
        // Grab the global Redis client
        synchronized (HoodieRedisIndex.class) {
          if (client == null) {
            client = RedisClientFactory.getClient(configuration);
          }
        }

        HoodieRecord<R> record = hoodieRecordIterator.next();
        byte[] key = OBJECT_MAPPER.writeValueAsBytes(record.getKey());
        if (client.exists(key)) {
          record.unseal();
          byte[] value = client.get(key);
          HoodieRecordLocation location = OBJECT_MAPPER.readValue(value, HoodieRecordLocation.class);
          record.setCurrentLocation(location);
          record.seal();
        }
        taggedRecords.add(record);
      }
      return taggedRecords.iterator();
    }, true);
  }

  @Override
  public HoodieData<WriteStatus> updateLocation(
      HoodieData<WriteStatus> writeStatuses, HoodieEngineContext context,
      HoodieTable hoodieTable) {
    return writeStatuses.map(writeStatus -> {
      // Grab the global Redis client
      synchronized (HoodieRedisIndex.class) {
        if (client == null) {
          client = RedisClientFactory.getClient(configuration);
        }
      }

      long numOfInserts = writeStatus.getStat().getNumInserts();
      LOG.info("Num of inserts in this WriteStatus: " + numOfInserts);

      for (HoodieRecord rec : writeStatus.getWrittenRecords()) {
        if (!writeStatus.isErrored(rec.getKey())) {
          Option<HoodieRecordLocation> newLocation = rec.getNewLocation();
          byte[] key = OBJECT_MAPPER.writeValueAsBytes(rec.getKey());
          if (newLocation.isPresent()) {
            if (rec.getCurrentLocation() != null) {
              // This is an update, no need to update index
              continue;
            }

            byte[] value = OBJECT_MAPPER.writeValueAsBytes(newLocation.get());
            client.set(key, value, expireTime);
          } else {
            // Delete existing index for a deleted record
            client.delete(key);
          }
        }
      }
      return writeStatus;
    });
  }

  @Override
  public boolean rollbackCommit(String instantTime) {
    return true;
  }

  /**
   * Only looks up by recordKey.
   */
  @Override
  public boolean isGlobal() {
    return true;
  }

  /**
   * Mapping is available in HBase already.
   */
  @Override
  public boolean canIndexLogFiles() {
    return true;
  }

  /**
   * Index needs to be explicitly updated after storage write.
   */
  @Override
  public boolean isImplicitWithStorage() {
    return false;
  }
}
