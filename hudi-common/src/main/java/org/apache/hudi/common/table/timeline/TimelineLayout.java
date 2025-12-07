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
import org.apache.hudi.common.table.timeline.versioning.v1.CommitMetadataSerDeV1;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantFileNameGeneratorV1;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantGeneratorV1;
import org.apache.hudi.common.table.timeline.versioning.v1.TimelinePathProviderV1;
import org.apache.hudi.common.table.timeline.versioning.v1.TimelineV1Factory;
import org.apache.hudi.common.table.timeline.versioning.v2.CommitMetadataSerDeV2;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantFileNameGeneratorV2;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantFileNameParserV2;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantGeneratorV2;
import org.apache.hudi.common.table.timeline.versioning.v2.TimelinePathProviderV2;
import org.apache.hudi.common.table.timeline.versioning.v2.TimelineV2Factory;
import org.apache.hudi.common.util.collection.Pair;

import lombok.Getter;

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

  public static final TimelineLayout TIMELINE_LAYOUT_V0 = new TimelineLayoutV0();
  public static final TimelineLayout TIMELINE_LAYOUT_V1 = new TimelineLayoutV1();
  public static final TimelineLayout TIMELINE_LAYOUT_V2 = new TimelineLayoutV2();

  static {
    LAYOUT_MAP.put(new TimelineLayoutVersion(TimelineLayoutVersion.VERSION_0), TIMELINE_LAYOUT_V0);
    LAYOUT_MAP.put(new TimelineLayoutVersion(TimelineLayoutVersion.VERSION_1), TIMELINE_LAYOUT_V1);
    LAYOUT_MAP.put(new TimelineLayoutVersion(TimelineLayoutVersion.VERSION_2), TIMELINE_LAYOUT_V2);
  }

  public static TimelineLayout fromVersion(TimelineLayoutVersion version) {
    return LAYOUT_MAP.get(version);
  }

  public abstract Stream<HoodieInstant> filterHoodieInstants(Stream<HoodieInstant> instantStream);

  public abstract InstantGenerator getInstantGenerator();

  public abstract InstantFileNameGenerator getInstantFileNameGenerator();

  public abstract TimelineFactory getTimelineFactory();

  public abstract InstantComparator getInstantComparator();

  public abstract InstantFileNameParser getInstantFileNameParser();

  public abstract CommitMetadataSerDe getCommitMetadataSerDe();

  public abstract TimelinePathProvider getTimelinePathProvider();

  /**
   * Table Layout where state transitions are managed by renaming files.
   */
  private static class TimelineLayoutV0 extends TimelineLayout {

    @Getter
    private final InstantGenerator instantGenerator = new InstantGeneratorV1();
    @Getter
    private final InstantFileNameGenerator instantFileNameGenerator = new InstantFileNameGeneratorV1();
    @Getter
    private final TimelineFactory timelineFactory = new TimelineV1Factory(this);
    @Getter
    private final InstantComparator instantComparator = new InstantComparatorV1();
    private final InstantFileNameParser fileNameParser = new InstantFileNameParserV2();

    @Override
    public Stream<HoodieInstant> filterHoodieInstants(Stream<HoodieInstant> instantStream) {
      return instantStream;
    }

    @Override
    public InstantFileNameParser getInstantFileNameParser() {
      return fileNameParser;
    }

    @Override
    public CommitMetadataSerDe getCommitMetadataSerDe() {
      return new CommitMetadataSerDeV1();
    }

    @Override
    public TimelinePathProvider getTimelinePathProvider() {
      return new TimelinePathProviderV1();
    }
  }

  private static Stream<HoodieInstant> filterHoodieInstantsByLatestState(Stream<HoodieInstant> instantStream,
                                                                         Function<String, String> actionMapper) {
    return instantStream.collect(Collectors.groupingBy(instant -> Pair.of(instant.requestedTime(),
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

    @Getter
    private final InstantGenerator instantGenerator = new InstantGeneratorV2();
    @Getter
    private final InstantFileNameGenerator instantFileNameGenerator = new InstantFileNameGeneratorV2();
    @Getter
    private final TimelineFactory timelineFactory = new TimelineV2Factory(this);
    @Getter
    private final InstantComparator instantComparator = new InstantComparatorV2();
    private final InstantFileNameParser fileNameParser = new InstantFileNameParserV2();

    @Override
    public Stream<HoodieInstant> filterHoodieInstants(Stream<HoodieInstant> instantStream) {
      return TimelineLayout.filterHoodieInstantsByLatestState(instantStream, InstantComparatorV2::getComparableAction);
    }

    @Override
    public InstantFileNameParser getInstantFileNameParser() {
      return fileNameParser;
    }

    @Override
    public CommitMetadataSerDe getCommitMetadataSerDe() {
      return new CommitMetadataSerDeV2();
    }

    @Override
    public TimelinePathProvider getTimelinePathProvider() {
      return new TimelinePathProviderV2();
    }
  }
}
