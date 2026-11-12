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
import io.trino.plugin.hudi.HudiTableState.CommittedInstantTimes;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.read.TableState;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The handle is the only thing a worker gets from the coordinator about the table, so whatever the merge read path
 * needs must survive its JSON round trip.
 */
class TestHudiTableHandle
{
    private static final JsonCodec<HudiTableHandle> CODEC = jsonCodec(HudiTableHandle.class);

    private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"t\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}";

    @Test
    void testMergeOnReadHandleCarriesSchemaAndTableStateToTheWorker()
    {
        HudiTableState tableState = new HudiTableState(
                ImmutableMap.of(HoodieTableConfig.VERSION.key(), String.valueOf(HoodieTableVersion.SIX.versionCode())),
                Optional.of(new CommittedInstantTimes(
                        ImmutableList.of("20250101000000000", "20250102000000000"),
                        ImmutableList.of("20250103000000000"),
                        Optional.of("20250101000000000"))));

        HudiTableHandle handle = CODEC.fromJson(CODEC.toJson(handle(HoodieTableType.MERGE_ON_READ, SCHEMA, Optional.of(tableState))));

        assertThat(handle.getTableSchema().toString()).isEqualTo(SCHEMA);
        assertThat(handle.getTableState()).contains(tableState);
        HoodieTableConfig tableConfig = handle.getTableState().orElseThrow().toTableConfig();
        assertThat(tableConfig.getTableVersion()).isEqualTo(HoodieTableVersion.SIX);
        TableState readerState = handle.getTableState().orElseThrow().toTableState(handle.getBasePath(), tableConfig);
        assertThat(readerState.getBasePath().toString()).isEqualTo("/test/path");
        assertThat(readerState.isCommitted("20250102000000000")).isTrue();
        assertThat(readerState.isCommitted("20250103000000000")).as("inflight").isFalse();
        assertThat(readerState.isCommitted("20240101000000000")).as("before the timeline start").isTrue();
        assertThat(readerState.isCommitted("20250104000000000")).as("unknown").isFalse();
    }

    @Test
    void testCopyOnWriteHandleCarriesNeither()
    {
        HudiTableHandle handle = CODEC.fromJson(CODEC.toJson(handle(HoodieTableType.COPY_ON_WRITE, "", Optional.empty())));

        assertThat(handle.getTableSchema()).isNull();
        assertThat(handle.getTableState()).isEmpty();
    }

    @Test
    void testEachTableConfigIsAPrivateCopy()
    {
        HudiTableState tableState = new HudiTableState(ImmutableMap.of("hoodie.table.name", "t"), Optional.empty());

        HoodieTableConfig first = tableState.toTableConfig();
        first.setValue("hoodie.table.name", "changed");

        assertThat(tableState.toTableConfig().getString("hoodie.table.name")).isEqualTo("t");
    }

    private static HudiTableHandle handle(HoodieTableType tableType, String schema, Optional<HudiTableState> tableState)
    {
        return new HudiTableHandle(
                "test_schema",
                "test_table",
                "/test/path",
                tableType,
                ImmutableList.of(),
                ImmutableList.of(),
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                schema,
                tableState,
                "20250102000000000");
    }
}
