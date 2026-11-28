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

import io.trino.metastore.HiveType;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.testing.MaxRankRecordMerger;
import io.trino.plugin.hudi.testing.NonProjectionCompatibleRankMerger;
import io.trino.plugin.hudi.util.TrinoTypeMapper;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.UuidType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.collection.Pair;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hudi.HudiUtil.appendMissingSchemaColumns;
import static io.trino.plugin.hudi.HudiUtil.resolveMergeModeAndStrategyId;
import static io.trino.plugin.hudi.HudiUtil.toMetadataHiveType;
import static io.trino.plugin.hudi.HudiUtil.usesNonProjectionCompatibleMerger;
import static io.trino.plugin.hudi.HudiUtil.validateCustomMergeStrategyId;
import static io.trino.plugin.hudi.HudiUtil.withSchemaColumnTypes;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.hudi.common.model.HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link HudiUtil#toColumnHandle}, which types a {@link HiveColumnHandle} from a table-schema
 * field's Avro type, plus the merge-resolution helpers around it ({@link
 * HudiUtil#appendMissingSchemaColumns}, {@link HudiUtil#usesNonProjectionCompatibleMerger} and {@link
 * HudiUtil#validateCustomMergeStrategyId}). Used by the reader context and page-source provider to
 * resolve merge-required columns that are absent from the query projection (see apache/hudi#19249).
 */
class TestHudiUtilColumnHandles
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final TrinoTypeMapper TYPE_MAPPER = new TrinoTypeMapper(TYPE_OPERATORS);

    @Test
    public void testNullableStringMatchesHudiMetaColumnHandle()
    {
        // Hudi meta columns (e.g. _hoodie_commit_time) are ["null","string"] unions; the resolved handle
        // must be identical to the HIVE_STRING/VARCHAR handle prependHudiMetaAndOrderingColumns builds
        Schema nullableString = SchemaBuilder.unionOf().nullType().and().stringType().endUnion();
        HiveColumnHandle handle = toColumnHandle("_hoodie_commit_time", nullableString);

        assertThat(handle.getName()).isEqualTo("_hoodie_commit_time");
        assertThat(handle.getType()).isEqualTo(VARCHAR);
        assertThat(handle.getHiveType()).isEqualTo(HiveType.HIVE_STRING);
        assertThat(handle.getColumnType()).isEqualTo(HiveColumnHandle.ColumnType.REGULAR);
        assertThat(handle.isHidden()).isFalse();
    }

    @Test
    public void testPrimitiveTypes()
    {
        assertColumnHandle(Schema.create(Schema.Type.INT), INTEGER, HiveType.HIVE_INT);
        assertColumnHandle(Schema.create(Schema.Type.LONG), BIGINT, HiveType.HIVE_LONG);
        assertColumnHandle(Schema.create(Schema.Type.BOOLEAN), BOOLEAN, HiveType.HIVE_BOOLEAN);
        assertColumnHandle(Schema.create(Schema.Type.STRING), VARCHAR, HiveType.HIVE_STRING);
    }

    @Test
    public void testLogicalTypes()
    {
        assertColumnHandle(
                LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)),
                DATE, HiveType.HIVE_DATE);
        assertColumnHandle(
                LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)),
                TIMESTAMP_MICROS, HiveType.HIVE_TIMESTAMP);
        assertColumnHandle(
                LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Schema.Type.BYTES)),
                DecimalType.createDecimalType(10, 2), HiveType.valueOf("decimal(10,2)"));
        assertColumnHandle(
                LogicalTypes.decimal(10, 2).addToSchema(Schema.createFixed("fixed_dec", null, null, 5)),
                DecimalType.createDecimalType(10, 2), HiveType.valueOf("decimal(10,2)"));
    }

    @Test
    public void testNestedTypes()
    {
        HiveColumnHandle arrayHandle = toColumnHandle("arr", SchemaBuilder.array().items().stringType());
        assertThat(arrayHandle.getHiveType()).isEqualTo(HiveType.valueOf("array<string>"));

        HiveColumnHandle mapHandle = toColumnHandle("map", SchemaBuilder.map().values().intType());
        assertThat(mapHandle.getHiveType()).isEqualTo(HiveType.valueOf("map<string,int>"));

        HiveColumnHandle rowHandle = toColumnHandle("rec", SchemaBuilder.record("rec").fields()
                .requiredInt("a")
                .requiredString("b")
                .endRecord());
        assertThat(rowHandle.getHiveType()).isEqualTo(HiveType.valueOf("struct<a:int,b:string>"));
    }

    @Test
    public void testNullableUnionOfLogicalType()
    {
        Schema nullableDate = SchemaBuilder.unionOf().nullType().and()
                .type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))).endUnion();
        assertColumnHandle(nullableDate, DATE, HiveType.HIVE_DATE);
    }

    @Test
    public void testTypeWithoutHiveCounterpartThrows()
    {
        // Avro uuid maps to Trino UUID, which has no Hive counterpart; must fail with a clear error
        Schema uuid = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
        assertThatThrownBy(() -> toColumnHandle("id", uuid))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Unsupported Hive type");
    }

    @Test
    public void testAppendMissingSchemaColumns()
    {
        HoodieSchema dataSchema = HoodieSchema.fromAvroSchema(SchemaBuilder.record("rec").fields()
                .requiredString("col_a")
                .requiredLong("col_b")
                .requiredInt("col_c")
                .endRecord());
        HiveColumnHandle projected = HiveColumnHandle.createBaseColumn(
                "col_b", 1, HiveType.HIVE_LONG, BIGINT, HiveColumnHandle.ColumnType.REGULAR, Optional.empty());

        List<HiveColumnHandle> expanded = appendMissingSchemaColumns(dataSchema, List.of(projected));

        // Projection handles keep their order and instances; missing fields append in schema order
        assertThat(expanded).hasSize(3);
        assertThat(expanded.get(0)).isSameAs(projected);
        assertThat(expanded.get(1).getName()).isEqualTo("col_a");
        assertThat(expanded.get(1).getType()).isEqualTo(VARCHAR);
        assertThat(expanded.get(2).getName()).isEqualTo("col_c");
        assertThat(expanded.get(2).getType()).isEqualTo(INTEGER);
    }

    @Test
    public void testAppendMissingSchemaColumnsMatchesCaseInsensitively()
    {
        HoodieSchema dataSchema = HoodieSchema.fromAvroSchema(SchemaBuilder.record("rec").fields()
                .requiredString("COL_A")
                .endRecord());
        HiveColumnHandle projected = HiveColumnHandle.createBaseColumn(
                "col_a", 0, HiveType.HIVE_STRING, VARCHAR, HiveColumnHandle.ColumnType.REGULAR, Optional.empty());

        assertThat(appendMissingSchemaColumns(dataSchema, List.of(projected)))
                .containsExactly(projected);
    }

    @Test
    public void testWithSchemaColumnTypes()
    {
        HoodieSchema tableSchema = HoodieSchema.createRecord("rec", null, null, List.of(
                HoodieSchemaField.of("_hoodie_record_key", HoodieSchema.createNullable(HoodieSchemaType.STRING), null, HoodieSchema.NULL_VALUE),
                HoodieSchemaField.of("Event_Time", HoodieSchema.createTimestampMicros(), null, null),
                HoodieSchemaField.of("id", HoodieSchema.createUUID(), null, null),
                HoodieSchemaField.of("dt", HoodieSchema.create(HoodieSchemaType.STRING), null, null)));
        HiveColumnHandle recordKey = HiveColumnHandle.createBaseColumn(
                "_hoodie_record_key", 0, HiveType.HIVE_STRING, VARCHAR, HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
        // hive_sync declares a timestamp-micros as bigint without support_timestamp, and a uuid as string
        HiveColumnHandle eventTime = HiveColumnHandle.createBaseColumn(
                "event_time", 1, HiveType.HIVE_LONG, BIGINT, HiveColumnHandle.ColumnType.REGULAR, Optional.of("when"));
        HiveColumnHandle id = HiveColumnHandle.createBaseColumn(
                "id", 2, HiveType.HIVE_STRING, VARCHAR, HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
        HiveColumnHandle notInSchema = HiveColumnHandle.createBaseColumn(
                "extra", 3, HiveType.HIVE_INT, INTEGER, HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
        // A partition column keeps its metastore type even when the schema carries a field of that name
        HiveColumnHandle partition = HiveColumnHandle.createBaseColumn(
                "dt", -1, HiveType.HIVE_DATE, DATE, HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.empty());
        HiveColumnHandle path = HiveColumnHandle.pathColumnHandle();

        List<HiveColumnHandle> typed = withSchemaColumnTypes(List.of(recordKey, eventTime, id, notInSchema, partition, path), tableSchema, TYPE_MAPPER);

        assertThat(typed).hasSize(6);
        assertThat(typed.get(0)).isEqualTo(recordKey);
        // The data columns take the mapper's type and keep the metastore's name, index, comment and Hive type
        assertThat(typed.get(1)).isEqualTo(HiveColumnHandle.createBaseColumn(
                "event_time", 1, HiveType.HIVE_LONG, TIMESTAMP_TZ_MICROS, HiveColumnHandle.ColumnType.REGULAR, Optional.of("when")));
        assertThat(typed.get(2)).isEqualTo(HiveColumnHandle.createBaseColumn(
                "id", 2, HiveType.HIVE_STRING, UuidType.UUID, HiveColumnHandle.ColumnType.REGULAR, Optional.empty()));
        assertThat(typed.get(3)).isSameAs(notInSchema);
        assertThat(typed.get(4)).isSameAs(partition);
        assertThat(typed.get(5)).isSameAs(path);
    }

    @Test
    public void testWithSchemaColumnTypesRejectsFieldsWithoutATrinoType()
    {
        HoodieSchema tableSchema = HoodieSchema.createRecord("rec", null, null, List.of(
                HoodieSchemaField.of("u", HoodieSchema.createUnion(HoodieSchema.create(HoodieSchemaType.INT), HoodieSchema.create(HoodieSchemaType.STRING)), null, null)));
        HiveColumnHandle handle = HiveColumnHandle.createBaseColumn(
                "u", 0, HiveType.HIVE_STRING, VARCHAR, HiveColumnHandle.ColumnType.REGULAR, Optional.empty());

        // Failing the query keeps the column visible rather than silently dropping it
        assertThatThrownBy(() -> withSchemaColumnTypes(List.of(handle), tableSchema, TYPE_MAPPER))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Trino has no union type");
    }

    @Test
    public void testToColumnHandleWithSchemaTypeMapper()
    {
        HoodieSchemaField field = HoodieSchemaField.of("ts", HoodieSchema.createTimestampMicros());
        // Without the mapper the legacy Avro-based typing applies: a timestamp without time zone
        assertThat(HudiUtil.toColumnHandle(field, Optional.empty()).getType()).isEqualTo(TIMESTAMP_MICROS);
        // With it the mapper's instant type, and the Hive type a Hive sync declares for it
        HiveColumnHandle handle = HudiUtil.toColumnHandle(field, Optional.of(TYPE_MAPPER));
        assertThat(handle.getName()).isEqualTo("ts");
        assertThat(handle.getType()).isEqualTo(TIMESTAMP_TZ_MICROS);
        assertThat(handle.getHiveType()).isEqualTo(HiveType.HIVE_TIMESTAMP);
        assertThat(handle.getColumnType()).isEqualTo(HiveColumnHandle.ColumnType.REGULAR);

        // The legacy typing has no Hive counterpart for uuid; the schema-typed one falls back to string
        HiveColumnHandle uuidHandle = HudiUtil.toColumnHandle(HoodieSchemaField.of("id", HoodieSchema.createUUID()), Optional.of(TYPE_MAPPER));
        assertThat(uuidHandle.getType()).isEqualTo(UuidType.UUID);
        assertThat(uuidHandle.getHiveType()).isEqualTo(HiveType.HIVE_STRING);
    }

    @Test
    public void testMetadataHiveTypeFallbacks()
    {
        // Types Hive can declare translate as they are
        assertThat(toMetadataHiveType(BIGINT)).isEqualTo(HiveType.HIVE_LONG);
        assertThat(toMetadataHiveType(new ArrayType(REAL))).isEqualTo(HiveType.valueOf("array<float>"));
        // Types it cannot fall back to what a Hive sync declares for them, at any depth
        assertThat(toMetadataHiveType(TIMESTAMP_TZ_MILLIS)).isEqualTo(HiveType.HIVE_TIMESTAMP);
        assertThat(toMetadataHiveType(TIMESTAMP_TZ_MICROS)).isEqualTo(HiveType.HIVE_TIMESTAMP);
        assertThat(toMetadataHiveType(TIME_MILLIS)).isEqualTo(HiveType.HIVE_INT);
        assertThat(toMetadataHiveType(TIME_MICROS)).isEqualTo(HiveType.HIVE_LONG);
        assertThat(toMetadataHiveType(UuidType.UUID)).isEqualTo(HiveType.HIVE_STRING);
        assertThat(toMetadataHiveType(RowType.from(List.of(
                RowType.field("id", UuidType.UUID),
                RowType.field("at", new ArrayType(TIME_MICROS)),
                RowType.field("tags", new MapType(VARCHAR, TIMESTAMP_TZ_MICROS, TYPE_OPERATORS))))))
                .isEqualTo(HiveType.valueOf("struct<id:string,at:array<bigint>,tags:map<string,timestamp>>"));
    }

    @Test
    public void testAppendMissingColumnsWithSchemaTypeMapper()
    {
        HoodieSchema dataSchema = HoodieSchema.createRecord("rec", null, null, List.of(
                HoodieSchemaField.of("col_a", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
                HoodieSchemaField.of("col_t", HoodieSchema.createTimeMillis(), null, null)));
        HiveColumnHandle projected = HiveColumnHandle.createBaseColumn(
                "col_a", 0, HiveType.HIVE_STRING, VARCHAR, HiveColumnHandle.ColumnType.REGULAR, Optional.empty());

        List<HiveColumnHandle> expanded = appendMissingSchemaColumns(dataSchema, List.of(projected), Optional.of(TYPE_MAPPER));

        assertThat(expanded).hasSize(2);
        assertThat(expanded.get(0)).isSameAs(projected);
        assertThat(expanded.get(1).getName()).isEqualTo("col_t");
        assertThat(expanded.get(1).getType()).isEqualTo(TIME_MILLIS);
        assertThat(expanded.get(1).getHiveType()).isEqualTo(HiveType.HIVE_INT);
    }

    @Test
    public void testUsesNonProjectionCompatibleMerger()
    {
        // Merger without the isProjectionCompatible override (interface default: false) -> full-schema read
        assertThat(usesNonProjectionCompatibleMerger(
                RecordMergeMode.CUSTOM,
                NonProjectionCompatibleRankMerger.MERGE_STRATEGY_ID,
                NonProjectionCompatibleRankMerger.class.getName()))
                .isTrue();

        // Projection-compatible merger stays on the projected fast path
        assertThat(usesNonProjectionCompatibleMerger(
                RecordMergeMode.CUSTOM,
                MaxRankRecordMerger.MERGE_STRATEGY_ID,
                MaxRankRecordMerger.class.getName()))
                .isFalse();

        // Unresolvable merger: no expansion here; the file-group reader fails loudly on its own
        assertThat(usesNonProjectionCompatibleMerger(
                RecordMergeMode.CUSTOM,
                NonProjectionCompatibleRankMerger.MERGE_STRATEGY_ID,
                ""))
                .isFalse();

        // Non-CUSTOM merge modes never trigger the full-schema read
        assertThat(usesNonProjectionCompatibleMerger(
                RecordMergeMode.EVENT_TIME_ORDERING,
                NonProjectionCompatibleRankMerger.MERGE_STRATEGY_ID,
                NonProjectionCompatibleRankMerger.class.getName()))
                .isFalse();
    }

    @Test
    public void testUsesNonProjectionCompatibleMergerWithoutStrategyId()
    {
        // A CUSTOM mode without a strategy id cannot resolve a merger: no expansion here (and no NPE from
        // createValidRecordMerger); the file-group reader then fails loudly on its own
        assertThat(usesNonProjectionCompatibleMerger(
                RecordMergeMode.CUSTOM, null, NonProjectionCompatibleRankMerger.class.getName()))
                .isFalse();
        assertThat(usesNonProjectionCompatibleMerger(
                RecordMergeMode.CUSTOM, "", NonProjectionCompatibleRankMerger.class.getName()))
                .isFalse();
    }

    @Test
    public void testValidateCustomMergeStrategyId()
    {
        // A version 8 table can resolve to CUSTOM merge mode with no persisted strategy id; the read must be
        // rejected with an actionable error rather than NPE inside createValidRecordMerger
        assertThatThrownBy(() -> validateCustomMergeStrategyId(null))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key());
        assertThatThrownBy(() -> validateCustomMergeStrategyId(""))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key());

        assertThatCode(() -> validateCustomMergeStrategyId(NonProjectionCompatibleRankMerger.MERGE_STRATEGY_ID))
                .doesNotThrowAnyException();
    }

    @Test
    public void testResolveMergeModeAndStrategyIdPassesThroughV9Configs()
    {
        HoodieTableConfig tableConfig = new HoodieTableConfig();
        tableConfig.setValue(HoodieTableConfig.VERSION, "9");
        tableConfig.setValue(HoodieTableConfig.RECORD_MERGE_MODE, RecordMergeMode.CUSTOM.name());
        tableConfig.setValue(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID, NonProjectionCompatibleRankMerger.MERGE_STRATEGY_ID);

        Pair<RecordMergeMode, String> resolved = resolveMergeModeAndStrategyId(tableConfig);
        assertThat(resolved.getLeft()).isEqualTo(RecordMergeMode.CUSTOM);
        assertThat(resolved.getRight()).isEqualTo(NonProjectionCompatibleRankMerger.MERGE_STRATEGY_ID);
    }

    @Test
    public void testResolveMergeModeAndStrategyIdInfersPreV8Configs()
    {
        // A 0.x table with a custom payload class persists neither merge mode nor strategy id; both must
        // be inferred, mirroring FileGroupReaderSchemaHandler.generateRequiredSchema (mode, below v9) and
        // HoodieReaderContext.initRecordMerger (strategy id, below v8). The inferred payload-based
        // strategy resolves HoodieAvroRecordMerger, which is not projection compatible, so such tables
        // take the full-schema read path even with no merger impls configured.
        HoodieTableConfig tableConfig = new HoodieTableConfig();
        tableConfig.setValue(HoodieTableConfig.VERSION, "6");
        tableConfig.setValue(HoodieTableConfig.PAYLOAD_CLASS_NAME, "com.example.CustomPayload");

        Pair<RecordMergeMode, String> resolved = resolveMergeModeAndStrategyId(tableConfig);
        assertThat(resolved.getLeft()).isEqualTo(RecordMergeMode.CUSTOM);
        assertThat(resolved.getRight()).isEqualTo(PAYLOAD_BASED_MERGE_STRATEGY_UUID);
        assertThat(usesNonProjectionCompatibleMerger(resolved.getLeft(), resolved.getRight(), "")).isTrue();
    }

    @Test
    public void testResolveMergeModeAndStrategyIdInfersCommitTimeForDefaultPayload()
    {
        HoodieTableConfig tableConfig = new HoodieTableConfig();
        tableConfig.setValue(HoodieTableConfig.VERSION, "6");
        tableConfig.setValue(HoodieTableConfig.PAYLOAD_CLASS_NAME, OverwriteWithLatestAvroPayload.class.getName());

        Pair<RecordMergeMode, String> resolved = resolveMergeModeAndStrategyId(tableConfig);
        assertThat(resolved.getLeft()).isEqualTo(RecordMergeMode.COMMIT_TIME_ORDERING);
        assertThat(usesNonProjectionCompatibleMerger(resolved.getLeft(), resolved.getRight(), "")).isFalse();
    }

    @Test
    public void testResolveMergeModeAndStrategyIdKeepsRawStrategyIdForV8()
    {
        // Version 8 tables infer the merge MODE (the schema handler gates on below-9) but NOT the
        // strategy id (initRecordMerger gates on below-8); the raw null strategy id must flow through
        // un-inferred and be rejected by usesNonProjectionCompatibleMerger instead of NPE-ing
        HoodieTableConfig tableConfig = new HoodieTableConfig();
        tableConfig.setValue(HoodieTableConfig.VERSION, "8");
        tableConfig.setValue(HoodieTableConfig.PAYLOAD_CLASS_NAME, "com.example.CustomPayload");

        Pair<RecordMergeMode, String> resolved = resolveMergeModeAndStrategyId(tableConfig);
        assertThat(resolved.getLeft()).isEqualTo(RecordMergeMode.CUSTOM);
        assertThat(resolved.getRight()).isNull();
        assertThat(usesNonProjectionCompatibleMerger(resolved.getLeft(), resolved.getRight(), "")).isFalse();
    }

    private static void assertColumnHandle(Schema avroSchema, Type expectedType, HiveType expectedHiveType)
    {
        HiveColumnHandle handle = toColumnHandle("col", avroSchema);
        assertThat(handle.getName()).isEqualTo("col");
        assertThat(handle.getType()).isEqualTo(expectedType);
        assertThat(handle.getHiveType()).isEqualTo(expectedHiveType);
    }

    private static HiveColumnHandle toColumnHandle(String name, Schema avroSchema)
    {
        return HudiUtil.toColumnHandle(HoodieSchemaField.of(name, HoodieSchema.fromAvroSchema(avroSchema)));
    }
}
