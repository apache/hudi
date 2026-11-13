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
package io.trino.plugin.hudi.split;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalLong;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHudiReadOptimizedSplits
{
    private static final String COMMIT_TIME = "20250625153731546";
    private static final List<HivePartitionKey> PARTITION_KEYS = ImmutableList.of();

    @Test
    public void testReadOptimizedSkipsLogOnlyFileGroup()
    {
        FileSlice slice = new FileSlice("partition", COMMIT_TIME, "log-only");
        slice.addLogFile(new HoodieLogFile(new StoragePathInfo(
                new StoragePath("/test/path/.log-only_" + COMMIT_TIME + ".log.1_0-0-0"), 100, false, (short) 0, 100, 1)));

        assertThat(new HudiSplitFactory(createTableHandle(HoodieTableType.COPY_ON_WRITE),
                HudiSplitWeightProvider.uniformStandardWeightProvider(), DataSize.of(128, MEGABYTE))
                .createSplits(PARTITION_KEYS, slice, COMMIT_TIME)).isEmpty();

        // Snapshot planning must retain log-only groups; only read-optimized reads skip them.
        assertThat(new HudiSplitFactory(createTableHandle(),
                HudiSplitWeightProvider.uniformStandardWeightProvider(), DataSize.of(128, MEGABYTE))
                .createSplits(PARTITION_KEYS, slice, COMMIT_TIME))
                .singleElement().satisfies(split -> {
                    assertThat(split.getBaseFile()).isEmpty();
                    assertThat(split.getLogFiles()).hasSize(1);
                });
    }

    private static HudiTableHandle createTableHandle()
    {
        return createTableHandle(HoodieTableType.MERGE_ON_READ);
    }

    private static HudiTableHandle createTableHandle(HoodieTableType tableType)
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
                "",
                "101");
    }
}
