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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.hudi.HudiTableProperties.ORDERING_FIELDS_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.PRECOMBINE_FIELD_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHudiTableValidation
{
    @Test
    void testValidTable()
    {
        assertThatCode(() -> HudiTableValidation.validateCreateTable(metadata(
                columns("id", "event_time", "city"),
                ImmutableMap.of(
                        PRIMARY_KEY_PROPERTY, ImmutableList.of("id"),
                        ORDERING_FIELDS_PROPERTY, ImmutableList.of("event_time"),
                        PARTITIONED_BY_PROPERTY, ImmutableList.of("city")))))
                .doesNotThrowAnyException();
    }

    @Test
    void testRejectsEmptyAndReservedColumns()
    {
        assertThatThrownBy(() -> HudiTableValidation.validateCreateTable(metadata(ImmutableList.of(), ImmutableMap.of())))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("no columns");
        assertThatThrownBy(() -> HudiTableValidation.validateCreateTable(metadata(
                columns("id", "_hoodie_record_key"), ImmutableMap.of())))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("reserved");
    }

    @Test
    void testRejectsInvalidPartitionColumns()
    {
        assertThatThrownBy(() -> HudiTableValidation.validateCreateTable(metadata(
                columns("id", "city"),
                ImmutableMap.of(PARTITIONED_BY_PROPERTY, ImmutableList.of("missing")))))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("not present in the table schema");
        assertThatThrownBy(() -> HudiTableValidation.validateCreateTable(metadata(
                columns("city", "id"),
                ImmutableMap.of(PARTITIONED_BY_PROPERTY, ImmutableList.of("city")))))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("must be the last columns");
        assertThatThrownBy(() -> HudiTableValidation.validateCreateTable(metadata(
                columns("id", "city"),
                ImmutableMap.of(PARTITIONED_BY_PROPERTY, ImmutableList.of("city", "city")))))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("listed more than once");
        assertThatThrownBy(() -> HudiTableValidation.validateCreateTable(metadata(
                columns("id", "city"),
                ImmutableMap.of(PARTITIONED_BY_PROPERTY, ImmutableList.of("id", "city")))))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("only partition columns");
    }

    @Test
    void testRejectsInvalidRecordProperties()
    {
        assertThatThrownBy(() -> HudiTableValidation.validateCreateTable(metadata(
                columns("id", "city"),
                ImmutableMap.of(PRIMARY_KEY_PROPERTY, ImmutableList.of("missing")))))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Column 'missing' in primary_key");
        assertThatThrownBy(() -> HudiTableValidation.validateCreateTable(metadata(
                columns("id", "event_time"),
                ImmutableMap.of(ORDERING_FIELDS_PROPERTY, ImmutableList.of("missing")))))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Column 'missing' in ordering_fields");
        assertThatThrownBy(() -> HudiTableValidation.validateCreateTable(metadata(
                columns("id", "event_time"),
                ImmutableMap.of(
                        ORDERING_FIELDS_PROPERTY, ImmutableList.of("event_time"),
                        PRECOMBINE_FIELD_PROPERTY, "event_time"))))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot set both ordering_fields and precombine_field");
    }

    private static ConnectorTableMetadata metadata(List<ColumnMetadata> columns, Map<String, Object> properties)
    {
        return new ConnectorTableMetadata(new SchemaTableName("sales", "trips"), columns, properties);
    }

    private static List<ColumnMetadata> columns(String... names)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (String name : names) {
            columns.add(ColumnMetadata.builder()
                    .setName(name)
                    .setType(name.equals("city") ? VARCHAR : BIGINT)
                    .build());
        }
        return columns.build();
    }
}
