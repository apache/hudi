package org.apache.hudi.connect.core;

import java.util.concurrent.TimeUnit;

/**
 * Interface for the Global Coordinator that manages all the {@link TopicTransactionCoordinator}
 * that individually coordinate the transactions per Kafka Topic.
 */
public interface CoordinatorManager {

  void submitEvent(CoordinatorEvent event);

  void submitEvent(CoordinatorEvent event, long delay, TimeUnit unit);

  void processControlEvent(ControlEvent message);
}
