/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.common.util.queue;

/**
 * Producer for BoundedInMemoryQueue. Memory Bounded Buffer supports
 * multiple producers single consumer pattern.
 *
 * @param <I> Input type for buffer items produced
 */
public interface BoundedInMemoryQueueProducer<I> {

  /**
   * API to enqueue entries to memory bounded queue
   *
   * @param queue In Memory bounded queue
   */
  void produce(BoundedInMemoryQueue<I, ?> queue) throws Exception;
}
