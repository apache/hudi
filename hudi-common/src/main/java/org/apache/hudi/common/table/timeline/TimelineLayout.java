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

import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantFactoryV1;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantFileNameFactoryV1;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.timeline.versioning.v1.TimelineV1Factory;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantFactoryV2;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantFileNameFactoryV2;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantFileNameParserV2;
import org.apache.hudi.common.table.timeline.versioning.v2.TimelineV2Factory;
import org.apache.hudi.common.util.collection.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
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
    LAYOUT_MAP.put(new TimelineLayoutVersion(TimelineLayoutVersion.VERSION_2), new TimelineLayoutV2());
  }

  public static TimelineLayout getLayout(TimelineLayoutVersion version) {
    return LAYOUT_MAP.get(version);
  }

  public abstract Stream<HoodieInstant> filterHoodieInstants(Stream<HoodieInstant> instantStream);

  public abstract InstantFactory getInstantFactory();

  public abstract InstantFileNameFactory getInstantFileNameFactory();

  public abstract TimelineFactory getTimelineFactory();

  public abstract InstantComparator getInstantComparator();

  public abstract InstantFileNameParser getInstantFileNameParser();

  /**
   * Table Layout where state transitions are managed by renaming files.
   */
  private static class TimelineLayoutV0 extends TimelineLayout {

    private final InstantFactory instantFactory = new InstantFactoryV1();
    private final InstantFileNameFactory instantFileNameFactory = new InstantFileNameFactoryV1();
    private final TimelineFactory timelineFactory = new TimelineV1Factory(this);
    private final InstantComparator instantComparator = new InstantComparatorV1();
    private final InstantFileNameParser fileNameParser = new InstantFileNameParserV2();

    @Override
    public Stream<HoodieInstant> filterHoodieInstants(Stream<HoodieInstant> instantStream) {
      return instantStream;
    }

    @Override
    public InstantFactory getInstantFactory() {
      return instantFactory;
    }

    @Override
    public InstantFileNameFactory getInstantFileNameFactory() {
      return instantFileNameFactory;
    }

    @Override
    public TimelineFactory getTimelineFactory() {
      return timelineFactory;
    }

    @Override
    public InstantComparator getInstantComparator() {
      return instantComparator;
    }

    @Override
    public InstantFileNameParser getInstantFileNameParser() {
      return fileNameParser;
    }
  }

  private static Stream<HoodieInstant> filterHoodieInstantsByLatestState(Stream<HoodieInstant> instantStream,
                                                                         Function<String, String> actionMapper) {
    return instantStream.collect(Collectors.groupingBy(instant -> Pair.of(instant.getRequestTime(),
            actionMapper.apply(instant.getAction())))).values().stream()
        .map(hoodieInstants -> hoodieInstants.stream().reduce((x, y) -> {
          // Pick the one with the highest state
          if (x.getState().compareTo(y.getState()) >= 0) {
            return x;
          }
          return y;
        }).get());
  }

  /**
   * Table Layout where state transitions are managed by creating new files.
   */
  private static class TimelineLayoutV1 extends TimelineLayoutV0 {

    @Override
    public Stream<HoodieInstant> filterHoodieInstants(Stream<HoodieInstant> instantStream) {
      return TimelineLayout.filterHoodieInstantsByLatestState(instantStream, InstantComparatorV1::getComparableAction);
    }
  }

  /**
   * Timeline corresponding to Hudi 1.x
   */
  private static class TimelineLayoutV2 extends TimelineLayout {

    private final InstantFactory instantFactory = new InstantFactoryV2();
    private final InstantFileNameFactory instantFileNameFactory = new InstantFileNameFactoryV2();
    private final TimelineFactory timelineFactory = new TimelineV2Factory(this);
    private final InstantComparator instantComparator = new InstantComparatorV2();
    private final InstantFileNameParser fileNameParser = new InstantFileNameParserV2();

    @Override
    public Stream<HoodieInstant> filterHoodieInstants(Stream<HoodieInstant> instantStream) {
      return TimelineLayout.filterHoodieInstantsByLatestState(instantStream, InstantComparatorV2::getComparableAction);
    }

    @Override
    public InstantFactory getInstantFactory() {
      return instantFactory;
    }

    @Override
    public InstantFileNameFactory getInstantFileNameFactory() {
      return instantFileNameFactory;
    }

    @Override
    public TimelineFactory getTimelineFactory() {
      return timelineFactory;
    }

    @Override
    public InstantComparator getInstantComparator() {
      return instantComparator;
    }

    @Override
    public InstantFileNameParser getInstantFileNameParser() {
      return fileNameParser;
    }
  }
}
