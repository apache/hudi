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

package org.apache.hudi.metaserver.service;

import org.apache.hudi.metaserver.thrift.MetaException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * AOP for meta server.
 */
public class HoodieMetaServerProxyHandler implements InvocationHandler {
  private static final Logger LOG = LogManager.getLogger(HoodieMetaServerProxyHandler.class);

  private HoodieMetaServerService metaServerService;

  public HoodieMetaServerProxyHandler(HoodieMetaServerService metaServerService) {
    this.metaServerService = metaServerService;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    Throwable err = null;
    try {
      Object res = method.invoke(metaServerService, args);
      return res;
    } catch (IllegalAccessException | InvocationTargetException e) {
      err = e.getCause();
    } catch (Throwable e) {
      err = e;
    }
    if (err != null) {
      LOG.error("Call hudi meta server method=" + method.getName() + " args=" +  Arrays.toString(args) + " error", err);
      if (err instanceof TException) {
        throw err;
      } else {
        throw new MetaException(err.getMessage());
      }
    }
    return null;
  }
}
