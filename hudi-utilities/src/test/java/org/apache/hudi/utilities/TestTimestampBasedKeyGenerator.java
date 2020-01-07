package org.apache.hudi.utilities;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.SchemaTestUtil;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.utilities.keygen.TimestampBasedKeyGenerator;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestTimestampBasedKeyGenerator {
    private Schema schema = SchemaTestUtil.getTimestampEvolvedSchema();
    private GenericRecord baseRecord = null;

    public TestTimestampBasedKeyGenerator() throws IOException {
    }

    @Before
    public void initialize() throws IOException {
        baseRecord = SchemaTestUtil
                .generateAvroRecordFromJson(schema, 1, "001", "f1");
    }

    private TypedProperties getKeyConfig(String recordKeyFieldName, String partitionPathField, String hiveStylePartitioning) {
        TypedProperties props = new TypedProperties();
        props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), recordKeyFieldName);
        props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), partitionPathField);
        props.setProperty(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY(), hiveStylePartitioning);
        props.setProperty("hoodie.deltastreamer.keygen.timebased.timestamp.type", "EPOCHMILLISECONDS");
        props.setProperty("hoodie.deltastreamer.keygen.timebased.output.dateformat", "yyyy-MM-dd hh");
        props.setProperty("hoodie.deltastreamer.keygen.timebased.timezone", "GMT+8:00");
        return props;
    }

    private TypedProperties getKeyConfig2(String recordKeyFieldName, String partitionPathField, String hiveStylePartitioning) {
        TypedProperties props = getKeyConfig(recordKeyFieldName, partitionPathField, hiveStylePartitioning);
        props.setProperty("hoodie.deltastreamer.keygen.timebased.timezone", "GMT");
        return props;
    }

    private TypedProperties getKeyConfig3(String recordKeyFieldName, String partitionPathField, String hiveStylePartitioning) {
        TypedProperties props = getKeyConfig(recordKeyFieldName, partitionPathField, hiveStylePartitioning);
        props.setProperty("hoodie.deltastreamer.keygen.timebased.timestamp.type", "DATE_STRING");
        props.setProperty("hoodie.deltastreamer.keygen.timebased.input.dateformat", "yyyy-MM-dd hh:mm:ss");
        props.setProperty("hoodie.deltastreamer.keygen.timebased.timezone", "GMT+8:00");
        return props;
    }


    @Test
    public void testTimestampBasedKeyGenerator() {
        // if timezone is GMT+8:00
        baseRecord.put("createTime",1578283932000L);
        HoodieKey hk1 = new TimestampBasedKeyGenerator(getKeyConfig("field1", "createTime", "false")).getKey(baseRecord);
        assertEquals(hk1.getPartitionPath(), "2020-01-06 12");

        // if timezone is GMT
        HoodieKey hk2 = new TimestampBasedKeyGenerator(getKeyConfig2("field1", "createTime", "false")).getKey(baseRecord);
        assertEquals(hk2.getPartitionPath(), "2020-01-06 04");

        // if timestamp is DATE_STRING
        baseRecord.put("createTime","2020-01-06 12:12:12");
        HoodieKey hk3 = new TimestampBasedKeyGenerator(getKeyConfig3("field1", "createTime", "false")).getKey(baseRecord);
        assertEquals(hk3.getPartitionPath(), "2020-01-06 12");
    }


}
