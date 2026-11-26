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
package io.trino.plugin.hudi.testing;

import com.google.common.collect.ImmutableList;
import io.trino.metastore.Column;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;

/**
 * Creates a non-partitioned Merge-On-Read table with a decimal(40, 2) column, which Hudi writes but neither Trino
 * (decimals stop at precision 38) nor Hive can type. Hive sync cannot register it either, so the metastore columns
 * are registered by hand, with the decimal as a string.
 */
public class UntypeableFieldHudiTablesInitializer
        extends AbstractMergerHudiTablesInitializer
{
    public static final String TABLE_NAME = "untypeable_field_mor";

    private static final HoodieSchema WIDE_DECIMAL = HoodieSchema.createDecimal(40, 2);
    private static final Schema AVRO_SCHEMA = HoodieSchema.createRecord(TABLE_NAME, null, null, ImmutableList.of(
            HoodieSchemaField.of(RECORD_KEY_FIELD, HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of(ORDERING_FIELD, HoodieSchema.create(HoodieSchemaType.LONG)),
            HoodieSchemaField.of("amount", WIDE_DECIMAL)))
            .toAvroSchema();

    public UntypeableFieldHudiTablesInitializer()
    {
        super(TABLE_NAME);
    }

    @Override
    protected Schema avroSchema()
    {
        return AVRO_SCHEMA;
    }

    @Override
    protected List<Column> metastoreDataColumns()
    {
        return ImmutableList.of(
                new Column(RECORD_KEY_FIELD, HIVE_STRING, Optional.empty(), Map.of()),
                new Column(ORDERING_FIELD, HIVE_LONG, Optional.empty(), Map.of()),
                new Column("amount", HIVE_STRING, Optional.empty(), Map.of()));
    }

    @Override
    protected void writeInitialCommits(HoodieJavaWriteClient<HoodieAvroPayload> client)
    {
        GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
        record.put(RECORD_KEY_FIELD, "k1");
        record.put(ORDERING_FIELD, 100L);
        Schema decimal = WIDE_DECIMAL.toAvroSchema();
        record.put("amount", new Conversions.DecimalConversion().toBytes(new BigDecimal("12345678901234567890123456789012345678.90"), decimal, decimal.getLogicalType()));
        String commit = client.startCommit();
        client.commit(commit, client.bulkInsert(ImmutableList.of(avroRecord(record, "k1")), commit));
    }
}
