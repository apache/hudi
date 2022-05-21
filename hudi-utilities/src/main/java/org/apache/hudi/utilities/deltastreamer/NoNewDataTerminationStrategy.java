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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

/**
 * Post writer termination strategy for deltastreamer in continuous mode. This strategy is based on no new data for consecutive number of times.
 */
public class NoNewDataTerminationStrategy implements PostWriteTerminationStrategy {

  private static final Logger LOG = LogManager.getLogger(NoNewDataTerminationStrategy.class);

  public static final String MAX_ROUNDS_WITHOUT_NEW_DATA_TO_SHUTDOWN = "max.rounds.without.new.data.to.shutdown";
  public static final int DEFAULT_MAX_ROUNDS_WITHOUT_NEW_DATA_TO_SHUTDOWN = 3;

  private final int numTimesNoNewDataToShutdown;
  private int numTimesNoNewData = 0;

  public NoNewDataTerminationStrategy(TypedProperties properties) {
    numTimesNoNewDataToShutdown = properties.getInteger(MAX_ROUNDS_WITHOUT_NEW_DATA_TO_SHUTDOWN, DEFAULT_MAX_ROUNDS_WITHOUT_NEW_DATA_TO_SHUTDOWN);
  }

  @Override
  public boolean shouldShutdown(Option<JavaRDD<WriteStatus>> writeStatuses) {
    numTimesNoNewData = writeStatuses.isPresent() ? 0 : numTimesNoNewData + 1;
    if (numTimesNoNewData >= numTimesNoNewDataToShutdown) {
      LOG.info("Shutting down on continuous mode as there is no new data for " + numTimesNoNewData);
      return true;
    }
    return false;
  }
}
