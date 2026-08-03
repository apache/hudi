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

import io.trino.plugin.hudi.testing.SchemaEvolutionHudiTablesInitializer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hudi.testing.SchemaEvolutionHudiTablesInitializer.BIGINT_THRESHOLD;
import static io.trino.plugin.hudi.testing.SchemaEvolutionHudiTablesInitializer.DOUBLE_THRESHOLD;
import static io.trino.plugin.hudi.testing.SchemaEvolutionHudiTablesInitializer.FLOAT_TO_DOUBLE_COLUMN;
import static io.trino.plugin.hudi.testing.SchemaEvolutionHudiTablesInitializer.INT_TO_BIGINT_COLUMN;
import static io.trino.plugin.hudi.testing.SchemaEvolutionHudiTablesInitializer.INT_TO_VARCHAR_COLUMN;
import static io.trino.plugin.hudi.testing.SchemaEvolutionHudiTablesInitializer.TABLE_NAME;
import static io.trino.plugin.hudi.testing.SchemaEvolutionHudiTablesInitializer.VARCHAR_THRESHOLD;
import static io.trino.plugin.hudi.testing.SchemaEvolutionHudiTablesInitializer.expectedRowsFrom;

/**
 * apache/hudi#19457: a predicate on a column whose type was widened after a base file was written used to fail the
 * whole query with {@code Malformed Parquet file. Corrupted statistics for column ...}, because the domain carries
 * the metastore's widened type while the file's statistics are still of the original one. The connector now leaves
 * such a domain out of the parquet predicate, and the engine applies it above the scan as it always did.
 * <p>
 * Selecting the evolved column without constraining it was never affected, so every test here has to put a predicate
 * ON the evolved column -- and every projection has to include it, otherwise the domain would resolve to no
 * descriptor and be discarded for an unrelated reason, which is exactly the shape that passes against the unfixed
 * code. {@link #testReadingAnEvolvedColumnWithoutAPredicate} is the anchor that shows the read path itself was
 * always fine.
 *
 * @see TestHudiSchemaEvolutionPredicatesPositional for the same suite with columns resolved by ordinal
 */
public class TestHudiSchemaEvolutionPredicates
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HudiQueryRunner.builder()
                .addConnectorProperty("hudi.parquet.use-column-names", "true")
                .setDataLoader(new SchemaEvolutionHudiTablesInitializer())
                .build();
    }

    @Test
    public void testPredicateOnColumnEvolvedFromFloatToDouble()
    {
        assertQuery(selectWhere(FLOAT_TO_DOUBLE_COLUMN + " > " + DOUBLE_THRESHOLD), expectedRowsFrom(3));
    }

    @Test
    public void testPredicateOnColumnEvolvedFromIntToBigint()
    {
        assertQuery(selectWhere(INT_TO_BIGINT_COLUMN + " > " + BIGINT_THRESHOLD), expectedRowsFrom(4));
    }

    @Test
    public void testPredicateOnColumnEvolvedFromIntToVarchar()
    {
        assertQuery(selectWhere("%s > '%s'".formatted(INT_TO_VARCHAR_COLUMN, VARCHAR_THRESHOLD)), expectedRowsFrom(4));
    }

    @Test
    public void testPredicatesOnEvolvedAndUnevolvedColumnsTogether()
    {
        // The key column never evolved, so its domain is pushed down while the evolved column's is dropped. One
        // unusable domain must not cost the whole predicate its pushdown, nor the query its rows.
        assertQuery(
                selectWhere("%s > %s AND key >= 'k4'".formatted(FLOAT_TO_DOUBLE_COLUMN, DOUBLE_THRESHOLD)),
                expectedRowsFrom(4));
    }

    @Test
    public void testReadingAnEvolvedColumnWithoutAPredicate()
    {
        // The anchor: widening is a read-path feature that already worked, so a regression here would mean the
        // fixture stopped modelling an evolved table rather than that the guard misbehaved
        assertQuery(selectWhere("true"), expectedRowsFrom(1));
    }

    private static String selectWhere(String predicate)
    {
        return "SELECT key, %s, %s, %s FROM %s WHERE %s ORDER BY key".formatted(
                FLOAT_TO_DOUBLE_COLUMN, INT_TO_BIGINT_COLUMN, INT_TO_VARCHAR_COLUMN, TABLE_NAME, predicate);
    }
}
