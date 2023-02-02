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

package org.apache.hudi.connect.transaction;

import org.apache.hudi.connect.ControlMessage;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Interface for the Participant that
 * manages Writes for a
 * single Kafka partition, based on
 * coordination signals from the {@link TransactionCoordinator}.
 */
public interface TransactionParticipant {

  void start();

  void stop();

  void buffer(SinkRecord record);

  void processRecords();

  TopicPartition getPartition();

  void processControlEvent(ControlMessage message);

  long getLastKafkaCommittedOffset();
}
