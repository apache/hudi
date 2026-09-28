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
import io.trino.metastore.Column;
import io.trino.metastore.Table;
import io.trino.plugin.hudi.util.HudiSchemaConverter;
import io.trino.plugin.hudi.util.HudiTableTypeUtils;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Optional;

import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Guards the invariant this class exists for: the metastore descriptor and
 * {@code hoodie.table.create.schema} describe the same columns. {@code getColumnHandles} reads only
 * the metastore, so a mismatch is invisible until a query returns wrong columns -- or none, as in
 * HUDI-9435.
 */
final class TestHudiMetastoreTables
{
    private static final SchemaTableName TABLE_NAME = new SchemaTableName("sales", "trips");
    private static final String BASE_PATH = "memory:///warehouse/trips";

    @Test
    void testEveryHudiSchemaFieldIsRegistered()
    {
        // The HUDI-9435 guard. Data columns plus partition columns must account for every field in
        // the Hudi schema, with nothing invented and nothing dropped.
        HoodieSchema schema = schema();
        Table table = buildTable(HoodieTableType.COPY_ON_WRITE, ImmutableList.of("city"), schema);

        List<String> registered = ImmutableList.<Column>builder()
                .addAll(table.getDataColumns())
                .addAll(table.getPartitionColumns())
                .build().stream()
                .map(Column::getName)
                .toList();
        assertThat(registered)
                .containsExactlyInAnyOrderElementsOf(schema.getFields().stream().map(HoodieSchemaField::name).toList());
    }

    @Test
    void testColumnTypesComeFromTheSchema()
    {
        Table table = buildTable(HoodieTableType.COPY_ON_WRITE, ImmutableList.of(), schema());

        assertThat(columnType(table, "id")).isEqualTo(HIVE_LONG);
        assertThat(columnType(table, "city")).isEqualTo(HIVE_STRING);
    }

    @ParameterizedTest
    @EnumSource(HoodieTableType.class)
    void testInputFormatRoundTripsToTheTableType(HoodieTableType tableType)
    {
        // The input format is the only record of the table type in the metastore, and the only signal
        // isHudiTable uses. If this did not round-trip, getTableHandle would reject the table the
        // connector had just created.
        Table table = buildTable(tableType, ImmutableList.of(), schema());

        String inputFormat = table.getStorage().getStorageFormat().getInputFormat();
        assertThat(HudiTableTypeUtils.fromInputFormat(inputFormat)).isEqualTo(tableType);
    }

    @Test
    void testRegisteredAsExternalTable()
    {
        Table table = buildTable(HoodieTableType.COPY_ON_WRITE, ImmutableList.of(), schema());

        assertThat(table.getTableType()).isEqualTo(EXTERNAL_TABLE.name());
        assertThat(table.getParameters()).containsEntry("EXTERNAL", "TRUE");
        assertThat(table.getStorage().getLocation()).isEqualTo(BASE_PATH);
    }

    @Test
    void testBuildTableIncludesSparkDataSourceProvider()
    {
        Table table = buildTable(HoodieTableType.COPY_ON_WRITE, ImmutableList.of("city"), schema());

        assertThat(table.getParameters()).containsEntry("spark.sql.sources.provider", "hudi");
    }

    @ParameterizedTest
    @ValueSource(strings = {"time-millis", "uuid"})
    void testRejectsLogicalTypesWithoutHiveCounterpart(String logicalType)
    {
        Schema fieldType = switch (logicalType) {
            case "time-millis" -> LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
            case "uuid" -> LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
            default -> throw new AssertionError("Unexpected logical type: " + logicalType);
        };
        HoodieSchema schema = HoodieSchema.fromAvroSchema(SchemaBuilder.record("trips").fields()
                .name("value").type(fieldType).noDefault()
                .endRecord());

        assertThatThrownBy(() -> buildTable(HoodieTableType.COPY_ON_WRITE, ImmutableList.of(), schema))
                .isInstanceOf(TrinoException.class)
                .satisfies(failure -> assertThat(((TrinoException) failure).getErrorCode())
                        .isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageContaining("Unsupported Hive type");
    }

    private static Table buildTable(HoodieTableType tableType, List<String> partitionedBy, HoodieSchema schema)
    {
        return HudiMetastoreTables.buildTable(
                TABLE_NAME, BASE_PATH, tableType, schema, partitionedBy, true, Optional.of("public"), Optional.empty());
    }

    private static io.trino.metastore.HiveType columnType(Table table, String columnName)
    {
        return ImmutableList.<Column>builder()
                .addAll(table.getDataColumns())
                .addAll(table.getPartitionColumns())
                .build().stream()
                .filter(column -> column.getName().equals(columnName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no column " + columnName))
                .getType();
    }

    private static HoodieSchema schema()
    {
        return HudiSchemaConverter.toTableSchema(
                ImmutableList.of(
                        ColumnMetadata.builder().setName("id").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("event_time").setType(createTimestampType(6)).build(),
                        ColumnMetadata.builder().setName("city").setType(VARCHAR).build()),
                "trips");
    }
}
