/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.hadoop.realtime;

import com.google.common.collect.Lists;
import com.uber.hoodie.common.model.HoodieAvroPayload;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.table.log.HoodieCompactedLogRecordScanner;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIOException;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;

/**
 * Record Reader implementation to merge fresh avro data with base parquet data, to support real time
 * queries.
 */
public class HoodieRealtimeRecordReader implements RecordReader<NullWritable, ArrayWritable> {

    private final RecordReader<NullWritable, ArrayWritable> parquetReader;
    private final HoodieRealtimeFileSplit split;
    private final JobConf jobConf;

    public static final Log LOG = LogFactory.getLog(HoodieRealtimeRecordReader.class);

    private final HashMap<String, ArrayWritable> deltaRecordMap;
    private final MessageType baseFileSchema;

    public HoodieRealtimeRecordReader(HoodieRealtimeFileSplit split,
                                      JobConf job,
                                      RecordReader<NullWritable, ArrayWritable> realReader) {
        this.split = split;
        this.jobConf = job;
        this.parquetReader = realReader;
        this.deltaRecordMap = new HashMap<>();

        LOG.info("cfg ==> " + job.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR));
        try {
            baseFileSchema = readSchema(jobConf, split.getPath());
            readAndCompactLog();
        } catch (IOException e) {
            throw new HoodieIOException(
                "Could not create HoodieRealtimeRecordReader on path " + this.split.getPath(), e);
        }
    }

    /**
     * Reads the schema from the parquet file. This is different from ParquetUtils as it uses the
     * twitter parquet to support hive 1.1.0
     */
    private static MessageType readSchema(Configuration conf, Path parquetFilePath) {
        try {
            return ParquetFileReader.readFooter(conf, parquetFilePath).getFileMetaData()
                .getSchema();
        } catch (IOException e) {
            throw new HoodieIOException("Failed to read footer for parquet " + parquetFilePath,
                e);
        }
    }


    /**
     * Goes through the log files and populates a map with latest version of each key logged, since the base split was written.
     */
    private void readAndCompactLog() throws IOException {
        Schema writerSchema = new AvroSchemaConverter().convert(baseFileSchema);
        List<String> projectionFields = orderFields(
            jobConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR),
            jobConf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR),
            jobConf.get("partition_columns", ""));
        // TODO(vc): In the future, the reader schema should be updated based on log files & be able to null out fields not present before
        Schema readerSchema = generateProjectionSchema(writerSchema, projectionFields);

        LOG.info(
            String.format("About to read compacted logs %s for base split %s, projecting cols %s",
                split.getDeltaFilePaths(), split.getPath(), projectionFields));

        HoodieCompactedLogRecordScanner compactedLogRecordScanner =
            new HoodieCompactedLogRecordScanner(FSUtils.getFs(), split.getDeltaFilePaths(),
                readerSchema);

        // NOTE: HoodieCompactedLogRecordScanner will not return records for an in-flight commit
        // but can return records for completed commits > the commit we are trying to read (if using readCommit() API)
        for (HoodieRecord<HoodieAvroPayload> hoodieRecord : compactedLogRecordScanner) {
            GenericRecord rec = (GenericRecord) hoodieRecord.getData().getInsertValue(readerSchema)
                .get();
            String key = hoodieRecord.getRecordKey();
            // we assume, a later safe record in the log, is newer than what we have in the map & replace it.
            ArrayWritable aWritable = (ArrayWritable) avroToArrayWritable(rec, writerSchema);
            deltaRecordMap.put(key, aWritable);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Log record : " + arrayWritableToString(aWritable));
            }
        }
    }

    private static String arrayWritableToString(ArrayWritable writable) {
        if (writable == null) {
            return "null";
        }

        StringBuilder builder = new StringBuilder();
        Writable[] values = writable.get();
        builder.append(String.format("Size: %s,", values.length));
        for (Writable w: values) {
            builder.append(w + " ");
        }
        return builder.toString();
    }

    /**
     * Given a comma separated list of field names and positions at which they appear on Hive,
     * return a ordered list of field names, that can be passed onto storage.
     *
     * @param fieldNameCsv
     * @param fieldOrderCsv
     * @return
     */
    public static List<String> orderFields(String fieldNameCsv, String fieldOrderCsv,
        String partitioningFieldsCsv) {

        String[] fieldOrders = fieldOrderCsv.split(",");
        Set<String> partitioningFields = Arrays.stream(partitioningFieldsCsv.split(","))
            .collect(Collectors.toSet());
        List<String> fieldNames = Arrays.stream(fieldNameCsv.split(","))
            .filter(fn -> !partitioningFields.contains(fn)).collect(
                Collectors.toList());

        // Hive does not provide ids for partitioning fields, so check for lengths excluding that.
        if (fieldNames.size() != fieldOrders.length) {
            throw new HoodieException(String.format(
                "Error ordering fields for storage read. #fieldNames: %d, #fieldPositions: %d",
                fieldNames.size(), fieldOrders.length));
        }
        TreeMap<Integer, String> orderedFieldMap = new TreeMap<>();
        for (int ox = 0; ox < fieldOrders.length; ox++) {
            orderedFieldMap.put(Integer.parseInt(fieldOrders[ox]), fieldNames.get(ox));
        }
        return new ArrayList<>(orderedFieldMap.values());
    }

    /**
     * Generate a reader schema off the provided writeSchema, to just project out
     * the provided columns
     *
     * @param writeSchema
     * @param fieldNames
     * @return
     */
    public static Schema generateProjectionSchema(Schema writeSchema, List<String> fieldNames) {
        List<Schema.Field> projectedFields = new ArrayList<>();
        for (String fn: fieldNames) {
            Schema.Field field = writeSchema.getField(fn);
            if (field == null) {
                throw new HoodieException("Field "+ fn + " not found log schema. Query cannot proceed!");
            }
            projectedFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue()));
        }

        return Schema.createRecord(projectedFields);
    }

    /**
     * Convert the projected read from delta record into an array writable
     *
     * @param value
     * @param schema
     * @return
     */
    public static Writable avroToArrayWritable(Object value, Schema schema) {

        // if value is null, make a NullWritable
        if (value == null) {
            return NullWritable.get();
        }

        switch (schema.getType()) {
            case STRING:
                return new Text(value.toString());
            case BYTES:
                return new BytesWritable((byte[]) value);
            case INT:
                return new IntWritable((Integer) value);
            case LONG:
                return new LongWritable((Long) value);
            case FLOAT:
                return new FloatWritable((Float) value);
            case DOUBLE:
                return new DoubleWritable((Double) value);
            case BOOLEAN:
                return new BooleanWritable((Boolean) value);
            case NULL:
                return NullWritable.get();
            case RECORD:
                GenericRecord record = (GenericRecord) value;
                Writable[] values1 = new Writable[schema.getFields().size()];
                int index1 = 0;
                for (Schema.Field field : schema.getFields()) {
                    values1[index1++] = avroToArrayWritable(record.get(field.name()), field.schema());
                }
                return new ArrayWritable(Writable.class, values1);
            case ENUM:
                return new Text(value.toString());
            case ARRAY:
                Writable[] values2 = new Writable[schema.getFields().size()];
                int index2 = 0;
                for (Object obj : (GenericArray) value) {
                    values2[index2++] = avroToArrayWritable(obj, schema.getElementType());
                }
                return new ArrayWritable(Writable.class, values2);
            case MAP:
                // TODO(vc): Need to add support for complex types
                return NullWritable.get();
            case UNION:
                List<Schema> types = schema.getTypes();
                if (types.size() != 2) {
                    throw new IllegalArgumentException("Only support union with 2 fields");
                }
                Schema s1 = types.get(0);
                Schema s2 = types.get(1);
                if (s1.getType() == Schema.Type.NULL) {
                    return avroToArrayWritable(value, s2);
                } else if (s2.getType() == Schema.Type.NULL) {
                    return avroToArrayWritable(value, s1);
                } else {
                    throw new IllegalArgumentException("Only support union with null");
                }
            case FIXED:
                return new BytesWritable(((GenericFixed) value).bytes());
        }
        return null;
    }

    @Override
    public boolean next(NullWritable aVoid, ArrayWritable arrayWritable) throws IOException {
        // Call the underlying parquetReader.next - which may replace the passed in ArrayWritable with a new block of values
        boolean result = this.parquetReader.next(aVoid, arrayWritable);
        if(!result) {
            // if the result is false, then there are no more records
            return false;
        } else {
            // TODO(VC): Right now, we assume all records in log, have a matching base record. (which would be true until we have a way to index logs too)
            // return from delta records map if we have some match.
            String key = arrayWritable.get()[HoodieRealtimeInputFormat.HOODIE_RECORD_KEY_COL_POS].toString();
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("key %s, base values: %s, log values: %s",
                        key, arrayWritableToString(arrayWritable), arrayWritableToString(deltaRecordMap.get(key))));
            }
            if (deltaRecordMap.containsKey(key)) {
                Writable[] replaceValue = deltaRecordMap.get(key).get();
                Writable[] originalValue = arrayWritable.get();
                System.arraycopy(replaceValue, 0, originalValue, 0, originalValue.length);
                arrayWritable.set(originalValue);
            }
            return true;
        }
    }

    @Override
    public NullWritable createKey() {
        return parquetReader.createKey();
    }

    @Override
    public ArrayWritable createValue() {
        return parquetReader.createValue();
    }

    @Override
    public long getPos() throws IOException {
        return parquetReader.getPos();
    }

    @Override
    public void close() throws IOException {
        parquetReader.close();
    }

    @Override
    public float getProgress() throws IOException {
        return parquetReader.getProgress();
    }
}
