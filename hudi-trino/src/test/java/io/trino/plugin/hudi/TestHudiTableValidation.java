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
import io.trino.spi.type.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.hudi.HudiTableProperties.KEY_GENERATOR_CLASS_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.ORDERING_FIELDS_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
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

    @ParameterizedTest
    @ValueSource(strings = {"a-b", "a b", "1a"})
    void testRejectsColumnNamesThatAreNotValidAvroNames(String columnName)
    {
        assertThatThrownBy(() -> HudiTableValidation.validateCreateTable(metadata(
                columns(columnName), ImmutableMap.of())))
                .isInstanceOf(TrinoException.class)
                .satisfies(failure -> assertThat(((TrinoException) failure).getErrorCode())
                        .isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageContaining("Column name '" + columnName + "'")
                .hasMessageContaining("Avro names must match [A-Za-z_][A-Za-z0-9_]*");
    }

    @Test
    void testRejectsRowFieldNamesThatAreNotValidAvroNames()
    {
        List<ColumnMetadata> columns = ImmutableList.of(ColumnMetadata.builder()
                .setName("payload")
                .setType(RowType.rowType(RowType.field("a-b", BIGINT)))
                .build());

        assertThatThrownBy(() -> HudiTableValidation.validateCreateTable(metadata(columns, ImmutableMap.of())))
                .isInstanceOf(TrinoException.class)
                .satisfies(failure -> assertThat(((TrinoException) failure).getErrorCode())
                        .isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageContaining("Column name 'payload.a-b'")
                .hasMessageContaining("Avro names must match [A-Za-z_][A-Za-z0-9_]*");
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
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "org.apache.hudi.keygen.CustomKeyGenerator",
            "org.apache.hudi.keygen.CustomAvroKeyGenerator"})
    void testRejectsCustomKeyGenerators(String keyGeneratorClass)
    {
        assertThatThrownBy(() -> HudiTableValidation.validateCreateTable(metadata(
                columns("id", "city"), ImmutableMap.of(
                        KEY_GENERATOR_CLASS_PROPERTY, keyGeneratorClass,
                        PARTITIONED_BY_PROPERTY, ImmutableList.of("city")))))
                .isInstanceOf(TrinoException.class)
                .satisfies(failure -> assertThat(((TrinoException) failure).getErrorCode())
                        .isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageContaining(keyGeneratorClass)
                .hasMessageContaining("requires partition field types");
    }

    @Test
    void testSimpleKeyGeneratorIsAllowed()
    {
        assertThatCode(() -> HudiTableValidation.validateCreateTable(metadata(
                columns("id", "city"), ImmutableMap.of(
                        KEY_GENERATOR_CLASS_PROPERTY, "org.apache.hudi.keygen.SimpleKeyGenerator",
                        PARTITIONED_BY_PROPERTY, ImmutableList.of("city")))))
                .doesNotThrowAnyException();
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
