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

package org.apache.hudi.connect.core;

import org.apache.hudi.connect.writers.HudiConnectConfigs;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Interface for the Coordinator that
 * coordinates the write transactions
 * across all the Kafka partitions, that
 * are managed by the {@link TransactionParticipant}.
 */
public abstract class TransactionCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionCoordinator.class);
  private static final ExecutorService EXECUTOR_SERVICE;
  private static final AtomicBoolean IS_EXECUTOR_RUNNING;
  private static EventProcessor eventProcessor;

  private final TopicPartition partition;

  static {
    EXECUTOR_SERVICE = Executors.newSingleThreadExecutor();
    IS_EXECUTOR_RUNNING = new AtomicBoolean(false);
  }

  public TransactionCoordinator(HudiConnectConfigs configs, TopicPartition partition) {
    this.partition = partition;
    eventProcessor = EventProcessor.createInstance();
    eventProcessor.transactionCoordinatorMap.put(partition.topic(), this);
  }

  public void start() {
    if (IS_EXECUTOR_RUNNING.compareAndSet(false, true)) {
      EXECUTOR_SERVICE.submit(eventProcessor);
    }
  }

  public void stop() {
    eventProcessor.transactionCoordinatorMap.remove(partition.topic());
  }

  public TopicPartition getPartition() {
    return partition;
  }

  protected void submitEvent(CoordinatorEvent event) {
    submitEvent(event, 0, TimeUnit.MILLISECONDS);
  }

  protected void submitEvent(CoordinatorEvent event, long delay, TimeUnit unit) {
    eventProcessor.submitEvent(event, delay, unit);
  }

  public abstract void publishControlEvent(ControlEvent message);

  abstract void processCoordinatorEvent(CoordinatorEvent event);

  private static class EventProcessor implements Runnable {

    private static EventProcessor eventProcessor;

    private final Map<String, TransactionCoordinator> transactionCoordinatorMap;
    private final BlockingQueue<CoordinatorEvent> events;
    private final ScheduledExecutorService scheduler;

    public EventProcessor() {
      this.events = new LinkedBlockingQueue<>();
      this.transactionCoordinatorMap = new ConcurrentHashMap<>();
      this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    private static EventProcessor createInstance() {
      if (eventProcessor == null) {
        eventProcessor = new EventProcessor();
      }
      return eventProcessor;
    }

    private void submitEvent(CoordinatorEvent event, long delay, TimeUnit unit) {
      scheduler.schedule(() -> {
        events.add(event);
      }, delay, unit);
    }

    @Override
    public void run() {
      while (true) {
        try {
          CoordinatorEvent event = events.poll(100, TimeUnit.MILLISECONDS);
          if (event != null
              && transactionCoordinatorMap.containsKey(event.getTopicName())) {
            transactionCoordinatorMap.get(event.getTopicName()).processCoordinatorEvent(event);
          }
        } catch (InterruptedException exception) {
          LOG.warn("Error received while polling the event loop in Partition Coordinator", exception);
        }
      }
    }
  }
}
