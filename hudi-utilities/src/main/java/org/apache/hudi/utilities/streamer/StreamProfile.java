/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.streamer;

/**
 * A profile containing details about how the next input batch in StreamSync should be consumed and written.
 * For eg: KafkaStreamProfile contains number of events to consume in this sync round.
 * S3StreamProfile contains the list of files to consume in this sync round.
 * HudiIncrementalStreamProfile contains the beginInstant and endInstant commit times to consume in this sync round etc.
 *
 * @param <T> The type for source context, varies based on sourceType as described above.
 */
public interface StreamProfile<T> {

  /**
   * @return The maxBytes that will be consumed from the source in this sync round.
   */
  long getMaxSourceBytes();

  /**
   * @return The number of output partitions required in source RDD.
   */
  int getSourcePartitions();

  /**
   * @return The source specific context based on sourceType as described above.
   */
  T getSourceSpecificContext();
}
