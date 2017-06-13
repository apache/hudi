package com.uber.hoodie.common;

import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by kge on 2017-05-18.
 */
public class HoodieRowPayload implements HoodieRecordPayload<HoodieRowPayload> {
    private final Row row;
    private static Logger logger = LogManager.getLogger(HoodieRowPayload.class);

    public HoodieRowPayload(Row row) {
        this.row = row;
    }

    @Override
    public HoodieRowPayload preCombine(HoodieRowPayload another) {
        return this;
    }

    @Override
    public java.util.Optional<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema)
            throws IOException {
        return getInsertValue(schema);
    }

    @Override
    public java.util.Optional<IndexedRecord> getInsertValue(Schema schema) throws IOException {

        IndexedRecord record = new GenericData.Record(schema);
        Iterator<Schema.Field> iter = record.getSchema().getFields().iterator();
        while (iter.hasNext()) {
            Schema.Field field = iter.next();
            try {
                ((GenericRecord) record).put(field.name(), row.getAs(field.name()));

                // This should be where the insert value modifies field to false again
                ((GenericRecord) record).put(HoodieRecord.DELETE_FIELD, false);
                ((GenericRecord) record).put(HoodieRecord.DELETED_AT_FIELD, null);
            } catch (IllegalArgumentException e) {
            } catch (java.lang.RuntimeException e2) {
            }
        }
        return java.util.Optional.of(record);
    }
}
