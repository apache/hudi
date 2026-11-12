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

import io.trino.plugin.hudi.testing.SchemaChangeHudiTablesInitializer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hudi.testing.SchemaChangeHudiTablesInitializer.ADDED_COLUMN;
import static io.trino.plugin.hudi.testing.SchemaChangeHudiTablesInitializer.RT_TABLE_NAME;
import static io.trino.plugin.hudi.testing.SchemaChangeHudiTablesInitializer.TABLE_NAME;

/**
 * A Merge-On-Read snapshot read takes the table schema the coordinator resolves for each query and ships in the
 * table handle; nothing caches it across queries. This pins that: a column added between two queries, the way a
 * schema-on-write append followed by hive sync adds it, is read by the second query, from the log file that carries
 * it and as null for the records written before it. See {@link SchemaChangeHudiTablesInitializer}.
 * <p>
 * One test method only: it mutates the table.
 */
public class TestHudiMorSchemaChange
        extends AbstractTestQueryFramework
{
    private SchemaChangeHudiTablesInitializer writer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        writer = new SchemaChangeHudiTablesInitializer();
        return HudiQueryRunner.builder()
                .setDataLoader(writer)
                .build();
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }

    @Test
    public void testColumnAddedBetweenQueriesIsReadByTheNextQuery()
    {
        assertQuery(
                "SELECT key, name, ts FROM " + RT_TABLE_NAME + " ORDER BY key",
                "VALUES ('sc_k1', 'k1_updated', CAST(2 AS BIGINT)), ('sc_k2', 'k2_base', 1)");

        writer.addColumnAndUpsert();

        // sc_k1's last write predates the column; sc_k2's carries it, in a log file written with the evolved schema
        assertQuery(
                "SELECT key, name, ts, " + ADDED_COLUMN + " FROM " + RT_TABLE_NAME + " ORDER BY key",
                "VALUES ('sc_k1', 'k1_updated', CAST(2 AS BIGINT), CAST(NULL AS BIGINT)), ('sc_k2', 'k2_updated', 3, 42)");
        // The base file predates the column, so the read-optimized table reads it as null throughout
        assertQuery(
                "SELECT key, name, ts, " + ADDED_COLUMN + " FROM " + TABLE_NAME + " ORDER BY key",
                "VALUES ('sc_k1', 'k1_base', CAST(1 AS BIGINT), CAST(NULL AS BIGINT)), ('sc_k2', 'k2_base', 1, NULL)");
    }
}
