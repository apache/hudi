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

package com.uber.hoodie.common.util;

import com.uber.hoodie.avro.HoodieAvroWriteSupport;
import com.uber.hoodie.common.BloomFilter;
import com.uber.hoodie.common.model.HoodieRecord;

import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.HoodieIndexException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.uber.hoodie.common.util.FSUtils.getFs;

/**
 * Utility functions involving with parquet.
 */
public class ParquetUtils {

    /**
     * Read the rowKey list from the given parquet file.
     *
     * @param filePath    The parquet file path.
     */
    public static Set<String> readRowKeysFromParquet(Path filePath) {
        Configuration conf = new Configuration();
        conf.addResource(getFs().getConf());
        Schema readSchema = HoodieAvroUtils.getRecordKeySchema();
        AvroReadSupport.setAvroReadSchema(conf, readSchema);
        AvroReadSupport.setRequestedProjection(conf, readSchema);
        ParquetReader reader = null;
        Set<String> rowKeys = new HashSet<>();
        try {
            reader = AvroParquetReader.builder(filePath).withConf(conf).build();
            Object obj = reader.read();
            while (obj != null) {
                if (obj instanceof GenericRecord) {
                    rowKeys.add(((GenericRecord) obj).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString());
                }
                obj = reader.read();
            }
        } catch (IOException e) {
            throw new HoodieIOException("Failed to read row keys from Parquet " + filePath, e);

        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
        return rowKeys;
    }


    /**
     *
     * Read the metadata from a parquet file
     *
     * @param parquetFilePath
     * @return
     */
    public static ParquetMetadata readMetadata(Path parquetFilePath) {
        return readMetadata(new Configuration(), parquetFilePath);
    }

    public static ParquetMetadata readMetadata(Configuration conf, Path parquetFilePath) {
        ParquetMetadata footer;
        try {
            // TODO(vc): Should we use the parallel reading version here?
            footer = ParquetFileReader.readFooter(getFs().getConf(), parquetFilePath);
        } catch (IOException e) {
            throw new HoodieIOException("Failed to read footer for parquet " + parquetFilePath,
                    e);
        }
        return footer;
    }


    /**
     * Get the schema of the given parquet file.
     *
     * @param parquetFilePath
     * @return
     */
    public static MessageType readSchema(Path parquetFilePath) {
        return readMetadata(parquetFilePath).getFileMetaData().getSchema();
    }



    /**
     * Read out the bloom filter from the parquet file meta data.
     */
    public static BloomFilter readBloomFilterFromParquetMetadata(Path parquetFilePath) {
        ParquetMetadata footer = readMetadata(parquetFilePath);
        Map<String, String> metadata = footer.getFileMetaData().getKeyValueMetaData();
        if (metadata.containsKey(HoodieAvroWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY)) {
            return new BloomFilter(metadata.get(HoodieAvroWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY));
        } else {
            throw new HoodieIndexException("Could not find index in Parquet footer. Looked for key "
                + HoodieAvroWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY + " in "
                + parquetFilePath);
        }
    }


    /**
     *
     * NOTE: This literally reads the entire file contents, thus should be used with caution.
     *
     * @param filePath
     * @return
     */
    public static List<GenericRecord> readAvroRecords(Path filePath) {
        ParquetReader reader = null;
        List<GenericRecord> records = new ArrayList<>();
        try {
            reader = AvroParquetReader.builder(filePath).build();
            Object obj = reader.read();
            while (obj != null) {
                if (obj instanceof GenericRecord) {
                    records.add(((GenericRecord) obj));
                }
                obj = reader.read();
            }
        } catch (IOException e) {
            throw new HoodieIOException("Failed to read avro records from Parquet " + filePath, e);

        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
        return records;
    }
}
