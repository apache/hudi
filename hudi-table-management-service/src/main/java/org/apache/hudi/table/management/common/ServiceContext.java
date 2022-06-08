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

package org.apache.hudi.table.management.common;

import org.apache.hudi.table.management.store.jdbc.InstanceDao;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ServiceContext {

  private static ConcurrentHashMap<String, String> runningInstance = new ConcurrentHashMap<>();

  public static void addRunningInstance(String instanceIdentifier, String threadIdentifier) {
    runningInstance.put(instanceIdentifier, threadIdentifier);
  }

  public static void removeRunningInstance(String instanceIdentifier) {
    runningInstance.remove(instanceIdentifier);
  }

  public static int getRunningInstanceNum() {
    return runningInstance.size();
  }

  public static List<String> getRunningInstanceInfo() {
    List<String> runningInfos = new ArrayList<>();
    for (Map.Entry<String, String> instance : runningInstance.entrySet()) {
      runningInfos.add("instance " + instance.getKey() + " execution on " + instance.getValue());
    }
    return runningInfos;
  }

  private static ConcurrentHashMap<String, Long> pendingInstances = new ConcurrentHashMap<>();

  public static boolean containsPendingInstant(String key) {
    return pendingInstances.containsKey(key);
  }

  public static void refreshPendingInstant(String key) {
    pendingInstances.put(key, System.currentTimeMillis());
  }

  public static void removePendingInstant(String key) {
    pendingInstances.remove(key);
  }

  public static ConcurrentHashMap<String, Long> getPendingInstances() {
    return pendingInstances;
  }

  public static InstanceDao getInstanceDao() {
    return ServiceContextHolder.INSTANCE_DAO;
  }

  private static class ServiceContextHolder {
    private static final InstanceDao INSTANCE_DAO = new InstanceDao();
  }

}
