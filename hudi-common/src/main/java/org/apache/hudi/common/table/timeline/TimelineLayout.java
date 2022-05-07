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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.collection.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Timeline Layout responsible for applying specific filters when generating timeline instants.
 */
public abstract class TimelineLayout implements Serializable {

  private static final Map<TimelineLayoutVersion, TimelineLayout> LAYOUT_MAP = new HashMap<>();

  static {
    LAYOUT_MAP.put(new TimelineLayoutVersion(TimelineLayoutVersion.VERSION_0), new TimelineLayoutV0());
    LAYOUT_MAP.put(new TimelineLayoutVersion(TimelineLayoutVersion.VERSION_1), new TimelineLayoutV1());
  }

  public static TimelineLayout getLayout(TimelineLayoutVersion version) {
    return LAYOUT_MAP.get(version);
  }

  public abstract Stream<HoodieInstant> filterHoodieInstants(Stream<HoodieInstant> instantStream);

  /**
   * Table Layout where state transitions are managed by renaming files.
   */
  private static class TimelineLayoutV0 extends TimelineLayout {

    @Override
    public Stream<HoodieInstant> filterHoodieInstants(Stream<HoodieInstant> instantStream) {
      return instantStream;
    }
  }

  /**
   * Table Layout where state transitions are managed by creating new files.
   */
  private static class TimelineLayoutV1 extends TimelineLayout {

    @Override
    public Stream<HoodieInstant> filterHoodieInstants(Stream<HoodieInstant> instantStream) {
      return instantStream.collect(Collectors.groupingBy(instant -> Pair.of(instant.getTimestamp(),
          HoodieInstant.getComparableAction(instant.getAction())))).values().stream()
          .map(hoodieInstants -> hoodieInstants.stream().reduce((x, y) -> {
            // Pick the one with the highest state
            if (x.getState().compareTo(y.getState()) >= 0) {
              return x;
            }
            return y;
          }).get());
    }
  }
}
