package org.apache.hudi.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.filter.BloomFilter;
import org.apache.hudi.common.bloom.filter.BloomFilterFactory;
import org.apache.hudi.common.bloom.filter.BloomFilterTypeCode;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.HoodieAvroUtils;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TestHoodieAvroWriteSupport {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testAddKey() throws IOException {
    List<String> rowKeys = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      rowKeys.add(UUID.randomUUID().toString());
    }
    String filePath = folder.getRoot() + "/test.parquet";
    Schema schema = HoodieAvroUtils.getRecordKeySchema();
    BloomFilter filter = BloomFilterFactory.createBloomFilter(
      1000, 0.0001, 10000,
      BloomFilterTypeCode.SIMPLE.name());
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(
      new AvroSchemaConverter().convert(schema), schema, filter);
    ParquetWriter writer = new ParquetWriter(new Path(filePath), writeSupport, CompressionCodecName.GZIP,
      120 * 1024 * 1024, ParquetWriter.DEFAULT_PAGE_SIZE);
    for (String rowKey : rowKeys) {
      GenericRecord rec = new GenericData.Record(schema);
      rec.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, rowKey);
      writer.write(rec);
      writeSupport.add(rowKey);
    }
    writer.close();
  }
}
