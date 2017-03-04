/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.uber.hoodie.avro.MercifulJsonConverter;
import com.uber.hoodie.common.model.HoodieRecordPayload;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.Map;
import java.util.Optional;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Example row change event based on some example data used by testcases. The data avro schema is
 * src/test/resources/schema1.
 */
public class TestRawTripPayload implements HoodieRecordPayload<TestRawTripPayload> {
    private transient static final ObjectMapper mapper = new ObjectMapper();
    private String partitionPath;
    private String rowKey;
    private byte[] jsonDataCompressed;
    private int dataSize;
    private boolean isDeleted;

    public TestRawTripPayload(Optional<String> jsonData, String rowKey, String partitionPath,
                              String schemaStr, Boolean isDeleted) throws IOException {
        if(jsonData.isPresent()) {
            this.jsonDataCompressed = compressData(jsonData.get());
            this.dataSize = jsonData.get().length();
        }
        this.rowKey = rowKey;
        this.partitionPath = partitionPath;
        this.isDeleted = isDeleted;
    }

    public TestRawTripPayload(String jsonData, String rowKey, String partitionPath,
                              String schemaStr)throws IOException {
        this(Optional.of(jsonData), rowKey, partitionPath, schemaStr, false);
    }

    public TestRawTripPayload(String jsonData) throws IOException {
        this.jsonDataCompressed = compressData(jsonData);
        this.dataSize = jsonData.length();
        Map<String, Object> jsonRecordMap = mapper.readValue(jsonData, Map.class);
        this.rowKey = jsonRecordMap.get("_row_key").toString();
        this.partitionPath = jsonRecordMap.get("time").toString().split("T")[0].replace("-", "/");
        this.isDeleted = false;
    }

    public String getPartitionPath() {
        return partitionPath;
    }


    @Override public TestRawTripPayload preCombine(TestRawTripPayload another) {
        return another;
    }

    @Override public Optional<IndexedRecord> combineAndGetUpdateValue(IndexedRecord oldRec, Schema schema) throws IOException {
        return this.getInsertValue(schema);
    }

    @Override public Optional<IndexedRecord> getInsertValue(Schema schema) throws IOException {
        if(isDeleted){
            return Optional.empty();
        } else {
            MercifulJsonConverter jsonConverter = new MercifulJsonConverter(schema);
            return Optional.of(jsonConverter.convert(getJsonData()));
        }
    }

    public String getRowKey() {
        return rowKey;
    }

    private String getJsonData() throws IOException {
        return unCompressData(jsonDataCompressed);
    }


    private byte[] compressData(String jsonData) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DeflaterOutputStream dos =
            new DeflaterOutputStream(baos, new Deflater(Deflater.BEST_COMPRESSION), true);
        try {
            dos.write(jsonData.getBytes());
        } finally {
            dos.flush();
            dos.close();
        }
        return baos.toByteArray();
    }


    private String unCompressData(byte[] data) throws IOException {
        InflaterInputStream iis = new InflaterInputStream(new ByteArrayInputStream(data));
        StringWriter sw = new StringWriter(dataSize);
        IOUtils.copy(iis, sw);
        return sw.toString();
    }
}
