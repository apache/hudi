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

import io.trino.plugin.hudi.testing.CompositeHudiTablesInitializer;
import io.trino.plugin.hudi.testing.OmittedMetaColumnsHudiTablesInitializer;
import io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hudi.testing.OmittedMetaColumnsHudiTablesInitializer.LATE_COLUMN;
import static io.trino.plugin.hudi.testing.OmittedMetaColumnsHudiTablesInitializer.SHADOWED_COLUMN;
import static io.trino.plugin.hudi.testing.OmittedMetaColumnsHudiTablesInitializer.THRESHOLD;
import static io.trino.plugin.hudi.testing.OmittedMetaColumnsHudiTablesInitializer.expectedRowsAboveThreshold;

public class TestHudiConnectorParquetColumnNamesTest
        extends TestHudiSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HudiQueryRunner.builder()
                .addConnectorProperty("hudi.parquet.use-column-names", "false")
                // The resource tables all register the Hudi meta columns in the metastore, so their metastore
                // ordinals already equal their physical ones and nothing here resolves a stale ordinal. The
                // second fixture is the one whose metastore omits them.
                .setDataLoader(new CompositeHudiTablesInitializer(
                        new ResourceHudiTablesInitializer(),
                        new OmittedMetaColumnsHudiTablesInitializer()))
                .build();
    }

    /**
     * apache/hudi#19387: with columns resolved positionally, a predicate handle carrying a metastore ordinal has to
     * be rebuilt on the file's physical ordinal before it is pushed into the parquet reader. Left unremapped, the
     * domain lands on whichever column physically sits at that ordinal -- here {@code shadowed_value}, whose values
     * are far below the threshold -- and the only row group is pruned, so the query returns nothing at all.
     * <p>
     * {@code shadowed_value} has to stay in the SELECT list: {@code descriptorsByPath} is derived from the
     * projection, so a domain resolving to a column the query does not read finds no descriptor and is dropped
     * instead of being misapplied. Narrowing this projection turns the test green against the unfixed code.
     * <p>
     * Only the {@code hudi.parquet.use-column-names=false} suite runs this; the name-based parent resolves the
     * predicate by name and was never affected.
     */
    @Test
    public void testPredicateOnColumnWithStaleMetastoreOrdinal()
    {
        assertQuery(
                "SELECT key, %s, %s FROM %s WHERE %s > %s ORDER BY key".formatted(
                        SHADOWED_COLUMN,
                        LATE_COLUMN,
                        OmittedMetaColumnsHudiTablesInitializer.TABLE_NAME,
                        LATE_COLUMN,
                        THRESHOLD),
                expectedRowsAboveThreshold());
    }
}
