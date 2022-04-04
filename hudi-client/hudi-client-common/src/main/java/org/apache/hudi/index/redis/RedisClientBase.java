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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.HashMap;
import java.util.Map;

public abstract class RedisClientBase implements RedisClient {
  private static final Logger LOG = LogManager.getLogger(RedisClientBase.class);

  private final Object lock = new Object();

  /**
   * The redis java client use in standalone.
   */
  protected Jedis jedis = null;

  /**
   * The redis java client use in cluster.
   */
  protected JedisCluster jedisCluster = null;

  /**
   * The redis pipeline.
   */
  protected Pipeline pipeline;

  /**
   * The maximum number of retries when an exception is caught.
   */
  protected int maxRetries;

  /**
   * The configuration.
   */
  protected HoodieWriteConfig configuration;

  public RedisClientBase(HoodieWriteConfig configuration, Boolean isClusterMode) {
    this.configuration = configuration;
    this.maxRetries = configuration.getRedisIndexMaxRetries();

    if (isClusterMode) {
      this.jedisCluster = RedisClientFactory.getClusterClient(configuration);
      this.pipeline = new JedisClusterPipeline(this.jedisCluster);
    } else {
      this.jedis = RedisClientFactory.getBinaryClient(configuration);
      this.pipeline = this.jedis.pipelined();
    }
  }

  public Map<byte[], byte[]> batchGet(byte[][] keys) {
    Map<byte[], byte[]> result = new HashMap<>();
    Map<byte[], Response<byte[]>> pipelineCache = new HashMap<>();

    synchronized (lock) {
      try {
        for (byte[] key : keys) {
          Response<byte[]> response = pipeline.get(key);
          pipelineCache.put(key, response);
        }

        pipeline.sync();

        for (byte[] key : keys) {
          if (pipelineCache.containsKey(key)) {
            result.put(key, pipelineCache.get(key).get());
          } else {
            result.put(key, null);
          }
        }
      } catch (Throwable t) {
        LOG.error("Error when doing batch get.", t);
        throw new RuntimeException("Error when doing batch get.", t);
      }
    }

    return result;
  }
}
