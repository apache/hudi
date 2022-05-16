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
 * An implementation of {@link RedisClient} for standalone redis mode.
 */
public class StandaloneRedisClient extends RedisClientBase {

  private static final Logger LOG = LogManager.getLogger(StandaloneRedisClient.class);

  public StandaloneRedisClient(HoodieWriteConfig configuration) {
    super(configuration, false);
  }

  @Override
  public Boolean exists(byte[] key) {
    return jedis.exists(key);
  }

  @Override
  public byte[] get(byte[] key) {
    for (int retry = 0; retry < maxRetries; retry++) {
      try {
        return jedis.get(key);
      } catch (Exception e) {
        LOG.error("Get value from redis failed", e);
        jedis.close();
        jedis = RedisClientFactory.getBinaryClient(configuration);
      }
    }

    return new byte[0];
  }

  @Override
  public Map<byte[], byte[]> batchGet(byte[][] keys) {
    return super.batchGet(keys);
  }

  @Override
  public void set(byte[] key, byte[] value, Duration expireTime) {
    int expireSeconds = (int) expireTime.getSeconds();

    for (int retry = 0; retry < maxRetries; retry++) {
      try {
        String status = expireSeconds > 0 ? jedis.setex(key, expireSeconds, value) : jedis.set(key, value);

        if (OK.compareToIgnoreCase(status) == 0) {
          return;
        } else {
          LOG.error("Redis returned message [{}] instead of \"OK\"." + status);
        }
      } catch (Exception e) {
        LOG.error("Set value to redis failed", e);

        jedis.close();
        jedis = RedisClientFactory.getBinaryClient(configuration);
      }
    }

    throw new RuntimeException("Could not properly set the key "
        + new String(key, StandardCharsets.UTF_8) + " with the value " + new String(value, StandardCharsets.UTF_8) + ".");
  }

  @Override
  public void delete(byte[] key) {
    for (int retry = 0; retry < maxRetries; retry++) {
      try {
        jedis.del(key);
      } catch (Exception e) {
        LOG.error("Delete key from redis failed.", e);

        jedis.close();
        jedis = RedisClientFactory.getBinaryClient(configuration);
      }
    }
  }

  @Override
  public void close() {
    if (jedis != null) {
      jedis.close();
    }
  }
}
