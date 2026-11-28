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

    @Override
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testOrderingMergesReadTrinoRowsWithSchemaColumnTypes(boolean schemaColumnTypes)
    {
        Session session = SessionBuilder.from(getSession()).withSchemaColumnTypesEnabled(schemaColumnTypes).build();
        for (String table : List.of(EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME, CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME)) {
            QueryId queryId = getQueryRunner().executeWithPlan(session, "SELECT key, name, value FROM " + table).queryId();
            long vectorizedSplits = getQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(queryId).getQueryStats().getOperatorSummaries().stream()
                    .map(operator -> operator.getConnectorMetrics().getMetrics().get(HudiVectorizedMorPageSource.VECTORIZED_MOR_SPLITS))
                    .filter(Objects::nonNull)
                    .mapToLong(metric -> ((Count<?>) metric).getTotal())
                    .sum();
            if (schemaColumnTypes) {
                assertThat(vectorizedSplits).as(table).isPositive();
            }
            else {
                assertThat(vectorizedSplits).as(table).isZero();
            }
        }
    }
}
