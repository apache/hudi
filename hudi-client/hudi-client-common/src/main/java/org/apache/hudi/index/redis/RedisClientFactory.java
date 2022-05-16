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

import org.apache.hudi.config.HoodieRedisIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.DateTimeUtils.parseDuration;

/**
 * The redis client factory.
 */
public final class RedisClientFactory {

  private static final String SPLIT_COMMA = ",";

  private RedisClientFactory() {
  }

  public static RedisClient getClient(HoodieWriteConfig configuration) {
    boolean useClusterMode = configuration.getRedisIndexClusterMode();
    if (!useClusterMode) {
      return new StandaloneRedisClient(configuration);
    } else {
      return new ClusterRedisClient(configuration);
    }
  }

  public static Jedis getBinaryClient(HoodieWriteConfig configuration) {
    Duration connectTimeout = parseDuration(configuration.getRedisIndexConnectTimeout());
    Duration socketTimeout = parseDuration(configuration.getRedisIndexSocketTimeout());
    String password = configuration.getRedisIndexPassword();

    HostAndPort hostAndPort = HostAndPort.parseString(configuration.getRedisIndexAddress());

    Jedis jedis = new Jedis(
        hostAndPort.getHost(),
        hostAndPort.getPort(),
        (int) connectTimeout.toMillis(),
        (int) socketTimeout.toMillis()
    );

    if (password != null && !password.isEmpty()) {
      jedis.auth(password);
    }

    return jedis;
  }

  public static JedisCluster getClusterClient(HoodieWriteConfig configuration) {
    Duration connectTimeout = parseDuration(configuration.getRedisIndexConnectTimeout());
    Duration socketTimeout = parseDuration(configuration.getRedisIndexSocketTimeout());
    String password = configuration.getRedisIndexPassword();

    Set<HostAndPort> hostAndPorts =
        Arrays.stream(configuration.getRedisIndexAddress().split(SPLIT_COMMA))
            .map(HostAndPort::parseString)
            .collect(Collectors.toSet());

    int maxRetries = configuration.getRedisIndexMaxRetries();
    int maxIdle = configuration.getRedisIndexClusterMaxIdle();
    int maxTotal = configuration.getRedisIndexClusterMaxTotal();
    Duration maxWaitTime = parseDuration(configuration.getString(HoodieRedisIndexConfig.CLUSTER_MAX_WAIT));

    //CHECKSTYLE:OFF
    org.apache.commons.pool2.impl.GenericObjectPoolConfig config =
        new org.apache.commons.pool2.impl.GenericObjectPoolConfig();
    config.setMaxIdle(maxIdle);
    config.setMaxTotal(maxTotal);
    config.setMaxWaitMillis(maxWaitTime.toMillis());
    config.setTestOnBorrow(true);
    config.setTestOnReturn(true);
    //CHECKSTYLE:ON

    if (password.isEmpty()) {
      return new JedisCluster(hostAndPorts,
          (int) connectTimeout.toMillis(),
          (int) socketTimeout.toMillis(),
          maxRetries,
          config);
    } else {
      return new JedisCluster(
          hostAndPorts,
          (int) connectTimeout.toMillis(),
          (int) socketTimeout.toMillis(),
          maxRetries,
          password,
          config
      );
    }
  }
}
