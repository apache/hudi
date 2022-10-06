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

package org.apache.hudi.common.util.queue;

import org.apache.hudi.common.util.Option;

import java.util.Iterator;

/**
 * HoodieMessageQueue holds an internal message queue, and control the behavior of
 * 1. insert record into internal message queue.
 * 2. get record from internal message queue.
 * 3. close internal message queue.
 */
public interface HoodieMessageQueue<I, O> extends Iterable<O>{

  /**
   * Get the size of inner message queue.
   */
  long size();

  /**
   * Insert a record into inner message queue.
   */
  void insertRecord(I t) throws Exception;

  /**
   * Read records from inner message queue.
   */
  Option<O> readNextRecord();

  void close();

  Iterator<O> iterator();
}
