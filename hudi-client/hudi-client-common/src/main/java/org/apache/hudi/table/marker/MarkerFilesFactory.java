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

package org.apache.hudi.table.marker;

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MarkerFilesFactory {
  private static final Logger LOG = LogManager.getLogger(MarkerFilesFactory.class);

  public static MarkerFiles get(MarkerIOMode mode, HoodieTable table, String instantTime) {
    LOG.info("Instantiated MarkerFiles with mode: " + mode.toString());
    switch (mode) {
      case DIRECT:
        return new DirectMarkerFiles(table, instantTime);
      case TIMELINE_BASED:
        return new TimelineBasedMarkerFiles(table, instantTime);
      default:
        throw new HoodieException("The marker IO mode \"" + mode.name() + "\" is not supported.");
    }
  }
}
