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

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

/**
 * An implementation of {@link RedisClient} for cluster redis mode.
 */
public class ClusterRedisClient extends RedisClientBase {
  private static final Logger LOG = LogManager.getLogger(ClusterRedisClient.class);

  public ClusterRedisClient(HoodieWriteConfig configuration) {
    super(configuration, true);
  }

  @Override
  public Boolean exists(byte[] key) {
    return jedisCluster.exists(key);
  }

  @Override
  public byte[] get(byte[] key) {
    return jedisCluster.get(key);
  }

  @Override
  public Map<byte[], byte[]> batchGet(byte[][] keys) {
    return super.batchGet(keys);
  }

  @Override
  public void set(byte[] key, byte[] value, Duration expireTime) {
    int expireSeconds = (int) expireTime.getSeconds();

    String status = expireSeconds > 0 ? jedisCluster.setex(key, expireSeconds, value) : jedisCluster.set(key, value);

    if (OK.compareToIgnoreCase(status) != 0) {
      throw new RuntimeException(String.format("Could not properly set the key %s with value %s. "
          + "Returned status=%s", new String(key, StandardCharsets.UTF_8), new String(value, StandardCharsets.UTF_8), status));
    }
  }

  @Override
  public void delete(byte[] key) {
    jedisCluster.del(key);
  }

  @Override
  public void close() throws Exception {
    jedisCluster.close();
  }
}
