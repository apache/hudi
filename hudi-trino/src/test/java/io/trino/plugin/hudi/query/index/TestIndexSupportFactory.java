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
package io.trino.plugin.hudi.query.index;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TestIndexSupportFactory
{
    private static final Schema TABLE_SCHEMA = SchemaBuilder.record("test").fields()
            .requiredString("Name")
            .requiredString("city")
            .endRecord();
    private static final Domain ALICE = Domain.singleValue(VARCHAR, utf8Slice("Alice"));

    @Test
    void testResolveFieldNamesUsesTableSchemaCasing()
    {
        TupleDomain<String> resolved = IndexSupportFactory.resolveFieldNames(
                TupleDomain.withColumnDomains(ImmutableMap.of(column("name"), ALICE)), TABLE_SCHEMA);
        assertThat(resolved).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of("Name", ALICE)));
    }

    @Test
    void testResolveFieldNamesDropsColumnsMissingFromTableSchema()
    {
        // e.g. a meta column synced to the metastore for a table that does not populate it
        TupleDomain<String> resolved = IndexSupportFactory.resolveFieldNames(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column("name"), ALICE,
                        column("_hoodie_commit_time"), Domain.notNull(VARCHAR))),
                TABLE_SCHEMA);
        assertThat(resolved).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of("Name", ALICE)));

        assertThat(IndexSupportFactory.resolveFieldNames(
                TupleDomain.withColumnDomains(ImmutableMap.of(column("_hoodie_commit_time"), Domain.notNull(VARCHAR))),
                TABLE_SCHEMA).isAll()).isTrue();
    }

    private static HiveColumnHandle column(String name)
    {
        return new HiveColumnHandle(name, 0, HIVE_STRING, VARCHAR, Optional.empty(), REGULAR, Optional.empty());
    }
}
