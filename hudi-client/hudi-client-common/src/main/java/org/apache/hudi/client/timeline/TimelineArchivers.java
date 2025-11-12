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

package org.apache.hudi.client.timeline;

import org.apache.hudi.client.timeline.versioning.v1.TimelineArchiverV1;
import org.apache.hudi.client.timeline.versioning.v2.TimelineArchiverV2;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieTable;

public class TimelineArchivers<T extends HoodieAvroPayload, I, K, O>  {

  public static <T extends HoodieAvroPayload, I, K, O>  HoodieTimelineArchiver<T, I, K, O> getInstance(TimelineLayoutVersion layoutVersion,
                                                                                                       HoodieWriteConfig config,
                                                                                                       HoodieTable<T, I, K, O> table) {
    if (layoutVersion.equals(TimelineLayoutVersion.LAYOUT_VERSION_0) || layoutVersion.equals(TimelineLayoutVersion.LAYOUT_VERSION_1)) {
      return new TimelineArchiverV1<>(config, table);
    } else if (layoutVersion.equals(TimelineLayoutVersion.LAYOUT_VERSION_2)) {
      return new TimelineArchiverV2<>(config, table);
    } else {
      throw new HoodieException("Unknown table layout version : " + layoutVersion.getVersion());
    }
  }
}
