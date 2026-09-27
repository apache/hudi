/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.read.FileGroupReaderTableState;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.storage.StoragePath;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHudiTableHandle
{
    private static final JsonCodec<HudiTableHandle> CODEC = jsonCodec(HudiTableHandle.class);
    private static final Map<String, String> TABLE_CONFIG = ImmutableMap.of(
            "hoodie.table.name", "test_table",
            "hoodie.table.type", "MERGE_ON_READ",
            "hoodie.table.version", "6");

    @Test
    void testWorkerTableStateFromJson()
    {
        HudiCommittedInstants committedInstants = new HudiCommittedInstants(
                ImmutableList.of("20240201000000000", "20240401000000000"),
                ImmutableList.of("20240301000000000"),
                Optional.of("20240201000000000"));
        HudiTableHandle handle = CODEC.fromJson(CODEC.toJson(createTableHandle(TABLE_CONFIG, Optional.of(committedInstants))));

        assertThat(handle.getTableConfig()).isEqualTo(TABLE_CONFIG);
        assertThat(handle.getCommittedInstants()).contains(committedInstants);
        FileGroupReaderTableState tableState = handle.getFileGroupReaderTableState();
        assertThat(tableState.getBasePath()).isEqualTo(new StoragePath("/test/path"));
        assertThat(tableState.getTableConfig().getTableName()).isEqualTo("test_table");
        assertThat(tableState.getTableConfig().getTableVersion()).isEqualTo(HoodieTableVersion.SIX);
        // Before the timeline start counts as committed (archived), inflight and unknown instants do not
        assertThat(tableState.isCommitted("20240101000000000")).isTrue();
        assertThat(tableState.isCommitted("20240401000000000")).isTrue();
        assertThat(tableState.isCommitted("20240301000000000")).isFalse();
        assertThat(tableState.isCommitted("20240215000000000")).isFalse();
    }

    @Test
    void testWorkerTableStateRequiresTableConfig()
    {
        HudiTableHandle handle = CODEC.fromJson(CODEC.toJson(createTableHandle(ImmutableMap.of(), Optional.empty())));

        assertThatThrownBy(handle::getFileGroupReaderTableState)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("carries no table config");
    }

    @Test
    void testCaptureCommittedInstantsUpToLatestCommit()
    {
        InstantGenerator instantGenerator = TimelineLayout.TIMELINE_LAYOUT_V1.getInstantGenerator();
        List<HoodieInstant> instants = ImmutableList.of(
                instantGenerator.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "20240201000000000"),
                instantGenerator.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "20240301000000000"),
                instantGenerator.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "20240401000000000"),
                instantGenerator.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "20240501000000000"),
                instantGenerator.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "20240601000000000"));
        HoodieTimeline timeline = TimelineLayout.TIMELINE_LAYOUT_V1.getTimelineFactory().createDefaultTimeline(instants.stream(), null);

        assertThat(HudiCommittedInstants.capture(timeline, "20240401000000000")).isEqualTo(new HudiCommittedInstants(
                ImmutableList.of("20240201000000000", "20240401000000000"),
                ImmutableList.of("20240301000000000"),
                Optional.of("20240201000000000")));
    }

    private static HudiTableHandle createTableHandle(Map<String, String> tableConfig, Optional<HudiCommittedInstants> committedInstants)
    {
        return new HudiTableHandle(
                "test_schema",
                "test_table",
                "/test/path",
                HoodieTableType.MERGE_ON_READ,
                ImmutableList.of(),
                ImmutableList.of(),
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                "",
                "20240401000000000",
                tableConfig,
                committedInstants);
    }
}
