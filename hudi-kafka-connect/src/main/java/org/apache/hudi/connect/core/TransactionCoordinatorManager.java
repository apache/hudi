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

import org.apache.hudi.connect.kafka.KafkaControlAgent;
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
 * The Global Coordinator that manages all the {@link TopicTransactionCoordinator}
 * that individually coordinate the transactions per Kafka Topic.
 */
public class TransactionCoordinatorManager implements CoordinatorManager, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionCoordinatorManager.class);
  private final Map<String, TopicTransactionCoordinator> coordinators;
  private final AtomicBoolean hasStarted = new AtomicBoolean(false);
  private final BlockingQueue<CoordinatorEvent> events;
  private final ExecutorService executorService;
  private final ScheduledExecutorService scheduler;

  public TransactionCoordinatorManager() {
    this.coordinators = new ConcurrentHashMap<>();
    this.events = new LinkedBlockingQueue<>();
    scheduler = Executors.newSingleThreadScheduledExecutor();
    executorService = Executors.newSingleThreadExecutor();
  }

  public void start() {
    if (hasStarted.compareAndSet(false, true)) {
      executorService.submit(this);
    }
  }

  public void stop() {
    coordinators.clear();
    hasStarted.set(false);
    if (executorService != null) {
      boolean terminated = false;
      try {
        LOG.info("Shutting down executor service.");
        executorService.shutdown();
        LOG.info("Awaiting termination.");
        terminated = executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        // ignored
      }

      if (!terminated) {
        LOG.warn(
            "Unclean Kafka Control Manager executor service shutdown ");
        executorService.shutdownNow();
      }
    }
  }

  public void addTopicCoordinator(TopicPartition partition,
                                  TopicTransactionCoordinator coordinator) {
    // Start lazily if there is at least one coordinator to manage
    start();
    coordinators.put(partition.topic(), coordinator);
  }

  public void removeTopicCoordinator(TopicPartition partition) {
    coordinators.remove(partition.topic());
  }

  @Override
  public void submitEvent(CoordinatorEvent event) {
    this.submitEvent(event, 0, TimeUnit.SECONDS);
  }

  @Override
  public void submitEvent(CoordinatorEvent event, long delay, TimeUnit unit) {
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
            && coordinators.containsKey(event.getTopicName())) {
          coordinators.get(event.getTopicName()).processCoordinatorEvent(event);
        }
      } catch (InterruptedException exception) {
        LOG.warn("Error received while polling the event loop in Partition Coordinator", exception);
      }
    }
  }

  @Override
  public void processControlEvent(ControlEvent message) {
    CoordinatorEvent.CoordinatorEventType type;
    if (message.getMsgType().equals(ControlEvent.MsgType.WRITE_STATUS)) {
      type = CoordinatorEvent.CoordinatorEventType.WRITE_STATUS;
    } else {
      LOG.warn("The Coordinator should not be receiving messages of type {}", message.getMsgType().name());
      return;
    }

    CoordinatorEvent event = new CoordinatorEvent(type,
        message.senderPartition().topic(),
        message.getCommitTime());
    event.setMessage(message);
    submitEvent(event);
  }
}
