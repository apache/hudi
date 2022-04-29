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

package org.apache.hudi.connect.kafka;

import org.apache.hudi.connect.ControlMessage;
import org.apache.hudi.connect.transaction.TransactionCoordinator;
import org.apache.hudi.connect.transaction.TransactionParticipant;

/**
 * Manages the Kafka consumer and producer for
 * the Kafka Control Topic that ensures coordination across the
 * {@link TransactionCoordinator} and {@link TransactionParticipant}s.
 */
public interface KafkaControlAgent {

  void registerTransactionParticipant(TransactionParticipant worker);

  void deregisterTransactionParticipant(TransactionParticipant worker);

  void registerTransactionCoordinator(TransactionCoordinator coordinator);

  void deregisterTransactionCoordinator(TransactionCoordinator coordinator);

  void publishMessage(ControlMessage message);
}
