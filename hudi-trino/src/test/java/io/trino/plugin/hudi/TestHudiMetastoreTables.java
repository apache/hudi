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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.sync.common.util.SparkDataSourceTableUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.hudi.common.model.HoodieRecord.COMMIT_TIME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
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
    void testMetaFieldsAreRegisteredAndLeadTheDataColumns()
    {
        Table table = buildTable(HoodieTableType.COPY_ON_WRITE, ImmutableList.of(), schema());

        List<String> dataColumns = table.getDataColumns().stream().map(Column::getName).toList();
        assertThat(dataColumns).startsWith(COMMIT_TIME_METADATA_FIELD);
        assertThat(dataColumns).contains(RECORD_KEY_METADATA_FIELD);
        // Registered as strings, matching what hive sync produces for Spark- and Flink-created tables.
        assertThat(columnType(table, COMMIT_TIME_METADATA_FIELD)).isEqualTo(HIVE_STRING);
    }

    @Test
    void testPartitionColumnsAreSplitOutInDeclaredOrder()
    {
        Table table = buildTable(HoodieTableType.COPY_ON_WRITE, ImmutableList.of("city", "id"), schema());

        assertThat(table.getPartitionColumns().stream().map(Column::getName).toList())
                .containsExactly("city", "id");
        assertThat(table.getDataColumns().stream().map(Column::getName).toList())
                .doesNotContain("city", "id");
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
    void testPartitionColumnMissingFromSchemaIsRegisteredAsString()
    {
        Table table = buildTable(HoodieTableType.COPY_ON_WRITE, ImmutableList.of("region"), schema());

        assertThat(table.getPartitionColumns().stream().map(Column::getName).toList())
                .containsExactly("region");
        assertThat(columnType(table, "region")).isEqualTo(HIVE_STRING);
    }

    @Test
    void testRepeatedPartitionColumnIsRejected()
    {
        assertThatThrownBy(() -> buildTable(HoodieTableType.COPY_ON_WRITE, ImmutableList.of("city", "city"), schema()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("listed more than once");
    }

    @Test
    void testSharedSparkDataSourcePropertiesAreUsableFromTheConnector()
    {
        // hudi-sync-common's SparkDataSourceTableUtils is the engine-neutral producer of the
        // properties that make a table recognisable to Spark SQL. It imports only
        // org.apache.hudi.common.* and java.util.*, and hudi-sync-common is already a compile
        // dependency, so the connector can call it rather than reimplementing it. Directly linked,
        // not reflectively loaded, so there is no classloader question here.
        Map<String, String> sparkProperties = SparkDataSourceTableUtils.getSparkTableProperties(
                ImmutableList.of("city"), "3.5.0", 4000, schema(), false);

        assertThat(sparkProperties).containsEntry("spark.sql.sources.provider", "hudi");
        assertThat(sparkProperties).containsEntry("spark.sql.create.version", "3.5.0");
        assertThat(sparkProperties).containsEntry("spark.sql.sources.schema.numPartCols", "1");
        assertThat(sparkProperties).containsEntry("spark.sql.sources.schema.partCol.0", "city");
        assertThat(sparkProperties).containsKey("spark.sql.sources.schema.numParts");
        // The reconstructible Spark schema carries the meta fields and the data columns.
        String sparkSchema = sparkProperties.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith("spark.sql.sources.schema.part."))
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .collect(java.util.stream.Collectors.joining());
        assertThat(sparkSchema).contains("_hoodie_commit_time").contains("id").contains("city");

        Map<String, String> serdeProperties = SparkDataSourceTableUtils.getSparkSerdeProperties(false, BASE_PATH);
        assertThat(serdeProperties).containsEntry("path", BASE_PATH);
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
