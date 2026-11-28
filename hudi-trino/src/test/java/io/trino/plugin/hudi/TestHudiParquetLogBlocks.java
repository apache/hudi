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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.plugin.hudi.testing.ParquetLogBlockHudiTablesInitializer;
import io.trino.plugin.hudi.util.FileOperationUtils.FileOperation;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.plugin.hudi.testing.ParquetLogBlockHudiTablesInitializer.RT_TABLE_NAME;
import static io.trino.plugin.hudi.testing.ParquetLogBlockHudiTablesInitializer.TABLE_NAME;
import static io.trino.plugin.hudi.util.FileOperationUtils.FileType.LOG;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.util.stream.Collectors.toCollection;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Snapshot reads of a Merge-On-Read table whose log files hold parquet data blocks, which the file-group reader
 * hands to the connector as {@code inlinefs://} paths. Written by {@link ParquetLogBlockHudiTablesInitializer}.
 */
// Sequential so that the file operations recorded for a query are only that query's
@Execution(SAME_THREAD)
public class TestHudiParquetLogBlocks
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HudiQueryRunner.builder()
                .setDataLoader(new ParquetLogBlockHudiTablesInitializer())
                // A split scheduled on the coordinator reads through its metadata file cache, which skips the traced reads
                .addConnectorProperty("hudi.metadata.cache.enabled", "false")
                .build();
    }

    @Test
    public void testReadOptimizedShowsBaseRows()
    {
        assertQuery(
                "SELECT key, name, value FROM " + TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', 'k1_base', CAST(10 AS BIGINT)), ('k2', 'k2_base', 20), ('k3', 'k3_base', 30),"
                        + " ('k4', 'k4_base', 40), ('k5', 'k5_base', 50), ('k6', 'k6_base', 60)");
    }

    @Test
    public void testSnapshotMergesParquetLogBlocks()
    {
        // k1: the second block's update wins over the first block's and the base row; k2: soft-deleted;
        // k3: the obsolete update loses; k4: updated; k5: hard-deleted; k6: untouched
        assertQuery(
                "SELECT key, name, value, ts FROM " + RT_TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', 'k1_second', CAST(111 AS BIGINT), CAST(300 AS BIGINT)), ('k3', 'k3_base', 30, 100),"
                        + " ('k4', 'k4_updated', 44, 150), ('k6', 'k6_base', 60, 100)");
    }

    @Test
    public void testNarrowProjectionMergesParquetLogBlocks()
    {
        // Neither the ordering field nor _hoodie_is_deleted is projected; the log reads must still carry them
        assertQuery(
                "SELECT key, value FROM " + RT_TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', CAST(111 AS BIGINT)), ('k3', 30), ('k4', 44), ('k6', 60)");
        assertQuery("SELECT count(*) FROM " + RT_TABLE_NAME, "VALUES 4");
    }

    @Test
    public void testPredicateOnMergedValue()
    {
        assertQuery(
                "SELECT key FROM " + RT_TABLE_NAME + " WHERE value > 40",
                "VALUES 'k1', 'k4', 'k6'");
    }

    @Test
    public void testCommitTimeOfMergedRows()
    {
        // k1 and k4 come from the same log commit, whose commit time differs from the untouched base row's
        assertQuery(
                "SELECT count(DISTINCT _hoodie_commit_time) FROM " + RT_TABLE_NAME + " WHERE key IN ('k1', 'k4')",
                "VALUES 1");
        assertQuery(
                "SELECT count(DISTINCT _hoodie_commit_time) FROM " + RT_TABLE_NAME + " WHERE key IN ('k1', 'k6')",
                "VALUES 2");
    }

    @Test
    public void testSmallParquetLogBlockIsReadInOneRequest()
    {
        // The table's log files hold 5 parquet data blocks
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        queryRunner.executeWithPlan(queryRunner.getDefaultSession(), "SELECT key, value FROM " + RT_TABLE_NAME);
        Multiset<FileOperation> logReads = queryRunner.getSpans().stream()
                .filter(span -> span.getName().startsWith("Input."))
                .map(FileOperation::create)
                .filter(operation -> operation.fileType() == LOG)
                .collect(toCollection(HashMultiset::create));
        assertMultisetsEqual(
                logReads,
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation("Input.readFully", LOG), 5)
                        .build());
    }
}
