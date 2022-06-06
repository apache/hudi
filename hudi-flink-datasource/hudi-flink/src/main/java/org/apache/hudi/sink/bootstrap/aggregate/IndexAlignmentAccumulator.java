/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.bootstrap.aggregate;

import org.apache.hudi.exception.HoodieIndexException;

import org.apache.flink.api.java.tuple.Tuple4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bootstrap and bucketAssign ready task id accumulator.
 */
public class IndexAlignmentAccumulator implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(IndexAlignmentAccumulator.class);
  private static final long serialVersionUID = 1L;

  private final Map<String, IndexAlignmentTaskDetails> readyTaskMap;

  public IndexAlignmentAccumulator() {
    this.readyTaskMap = new HashMap<>();
  }

  public void update(Tuple4<String, Integer, Integer, Long> taskDetails) {
    if (!readyTaskMap.containsKey(taskDetails.f0)) {
      readyTaskMap.put(taskDetails.f0, new IndexAlignmentTaskDetails(taskDetails.f1));
    } else if (readyTaskMap.get(taskDetails.f0).getParalleTasks() != taskDetails.f1) {
      throw new HoodieIndexException("No possible.");
    }
    readyTaskMap.get(taskDetails.f0).addSubTaskCount(taskDetails.f2, taskDetails.f3);
  }

  public Boolean isReady() {
    if (readyTaskMap.size() != 2) {
      LOG.info("Now readyTaskMap contains: {}", readyTaskMap);
      return false;
    }
    List<String> keys = new ArrayList<>(readyTaskMap.keySet());
    boolean isReady = isReady(readyTaskMap.get(keys.get(0)), readyTaskMap.get(keys.get(1)));
    if (!isReady) {
      LOG.info("Now readyTaskMap contains: {}", readyTaskMap);
    }
    return isReady;
  }

  public static boolean isReady(IndexAlignmentTaskDetails a, IndexAlignmentTaskDetails b) {
    if (a.getParalleTasks() != a.getSubTaskCount().size()
        || b.getParalleTasks() != b.getSubTaskCount().size()) {
      return false;
    }
    return a.getCount() == b.getCount();
  }

  public IndexAlignmentAccumulator merge(IndexAlignmentAccumulator acc) {
    if (acc == null) {
      return this;
    }

    acc.readyTaskMap.forEach((k,v) -> {
      if (!readyTaskMap.containsKey(k)) {
        readyTaskMap.put(k, v);
      } else {
        if (readyTaskMap.get(k).getParalleTasks() != v.getParalleTasks()) {
          throw new HoodieIndexException("No possible.");
        }
        readyTaskMap.get(k).getSubTaskCount().putAll(v.getSubTaskCount());
      }
    });
    return this;
  }
}
