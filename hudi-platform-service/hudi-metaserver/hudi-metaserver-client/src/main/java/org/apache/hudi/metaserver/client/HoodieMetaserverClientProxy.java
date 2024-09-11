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

package org.apache.hudi.metaserver.client;

import org.apache.hudi.common.config.HoodieMetaserverConfig;
import org.apache.hudi.common.util.RetryHelper;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;

/**
 * AOP for meta server client.
 */
public class HoodieMetaserverClientProxy implements InvocationHandler, Serializable {

  private final int retryLimit;
  private final long retryDelayMs;
  private final HoodieMetaserverConfig config;

  private HoodieMetaserverClientProxy(HoodieMetaserverConfig config) {
    this.retryLimit = config.getConnectionRetryLimit();
    this.retryDelayMs = config.getConnectionRetryDelay() * 1000L;
    this.config = config;
  }

  public static HoodieMetaserverClient getProxy(HoodieMetaserverConfig config) {
    HoodieMetaserverClientProxy handler = new HoodieMetaserverClientProxy(config);
    return (HoodieMetaserverClient) Proxy.newProxyInstance(HoodieMetaserverClientProxy.class.getClassLoader(),
        new Class[]{HoodieMetaserverClient.class}, handler);
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    try (HoodieMetaserverClient client = new HoodieMetaserverClientImp(config)) {
      return new RetryHelper<Object, Exception>(retryDelayMs, retryLimit, retryDelayMs, Exception.class.getName())
          .tryWith(() -> method.invoke(client, args)).start();
    } catch (IllegalAccessException | InvocationTargetException | UndeclaredThrowableException e) {
      throw e.getCause();
    }
  }
}
