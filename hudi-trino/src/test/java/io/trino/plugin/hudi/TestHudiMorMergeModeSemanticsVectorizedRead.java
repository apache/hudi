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

import io.trino.Session;
import io.trino.plugin.hudi.reader.HudiVectorizedMorPageSource;
import io.trino.plugin.hudi.testing.CommitTimeOrderingHudiTablesInitializer;
import io.trino.plugin.hudi.testing.EventTimeDeletesHudiTablesInitializer;
import io.trino.spi.QueryId;
import io.trino.spi.metrics.Count;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link TestHudiMorMergeModeSemantics} with {@code hudi.mor-vectorized-read.enabled} on: every read with schema column
 * types on takes the vectorized MOR read, matching log records by record key (the fixtures' log blocks carry no
 * positions), except the spilling tables, whose spilled log records keep the merge read.
 */
public class TestHudiMorMergeModeSemanticsVectorizedRead
        extends TestHudiMorMergeModeSemantics
{
    @Override
    protected Map<String, String> connectorProperties()
    {
        return Map.of("hudi.mor-vectorized-read.enabled", "true");
    }

    @Test
    public void testSpilledLogRecordsFallBackToTheMergeRead()
    {
        // The split's log records spill, so the load hands the split to the merge read, which reads them again
        Session session = SessionBuilder.from(getSession()).withSchemaColumnTypesEnabled(true).build();
        String sql = "SELECT key, name, value FROM " + CommitTimeOrderingHudiTablesInitializer.SPILLING_RT_TABLE_NAME;
        assertThat(metric(session, sql, HudiVectorizedMorPageSource.VECTORIZED_MOR_SPLITS)).isZero();
        assertThat(metric(session, sql, HudiPageSource.TRINO_ROW_MERGE_SPLITS)).isPositive();
        assertQuery(session, sql, "VALUES ('k1', 'k1_updated', CAST(11 AS BIGINT)), ('k3', 'k3_base', 30)");
    }

    private long metric(Session session, String sql, String name)
    {
        QueryId queryId = getQueryRunner().executeWithPlan(session, sql).queryId();
        return getQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(queryId).getQueryStats().getOperatorSummaries().stream()
                .map(operator -> operator.getConnectorMetrics().getMetrics().get(name))
                .filter(Objects::nonNull)
                .mapToLong(metric -> ((Count<?>) metric).getTotal())
                .sum();
    }

    @Override
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testOrderingMergesReadTrinoRowsWithSchemaColumnTypes(boolean schemaColumnTypes)
    {
        Session session = SessionBuilder.from(getSession()).withSchemaColumnTypesEnabled(schemaColumnTypes).build();
        for (String table : List.of(EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME, CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME)) {
            long vectorizedSplits = metric(session, "SELECT key, name, value FROM " + table, HudiVectorizedMorPageSource.VECTORIZED_MOR_SPLITS);
            if (schemaColumnTypes) {
                assertThat(vectorizedSplits).as(table).isPositive();
            }
            else {
                assertThat(vectorizedSplits).as(table).isZero();
            }
        }
    }
}
