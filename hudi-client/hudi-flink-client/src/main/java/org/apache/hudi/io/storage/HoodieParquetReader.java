package org.apache.hudi.io.storage;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.common.util.ParquetReaderIterator;
import org.apache.hudi.common.util.collection.ClosableIterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class HoodieParquetReader<R extends IndexedRecord> implements HoodieFileReader<R> {

  private final Path path;
  private final Configuration conf;
  private final BaseFileUtils parquetUtils;
  private List<ParquetReaderIterator> readerIterators = new ArrayList<>();

  public HoodieParquetReader(Configuration configuration, Path path) {
    this.conf = configuration;
    this.path = path;
    this.parquetUtils = BaseFileUtils.getInstance(HoodieFileFormat.PARQUET);
  }

  @Override
  public String[] readMinMaxRecordKeys() {
    return parquetUtils.readMinMaxRecordKeys(conf, path);
  }

  @Override
  public BloomFilter readBloomFilter() {
    return parquetUtils.readBloomFilterFromMetadata(conf, path);
  }

  @Override
  public Set<String> filterRowKeys(Set<String> candidateRowKeys) {
    return parquetUtils.filterRowKeys(conf, path, candidateRowKeys);
  }

  @Override
  public ClosableIterator<HoodieRecord<R>> getRecordIterator(Schema readerSchema, Schema requestedSchema) throws IOException {
    AvroReadSupport.setAvroReadSchema(conf, readerSchema);
    ParquetReader<HoodieRecord<R>> reader = AvroParquetReader.<HoodieRecord<R>>builder(path).withConf(conf).build();
    ParquetReaderIterator<HoodieRecord<R>> parquetReaderIterator = new ParquetReaderIterator<>(reader);
    readerIterators.add(parquetReaderIterator);
    return parquetReaderIterator;
  }

  @Override
  public ClosableIterator<HoodieRecord<R>> getRecordIterator(Schema schema) throws IOException {
    AvroReadSupport.setAvroReadSchema(conf, schema);
    ParquetReader<HoodieRecord<R>> reader = AvroParquetReader.<HoodieRecord<R>>builder(path).withConf(conf).build();
    ParquetReaderIterator<HoodieRecord<R>> parquetReaderIterator = new ParquetReaderIterator<>(reader);
    readerIterators.add(parquetReaderIterator);
    return parquetReaderIterator;
  }

  @Override
  public ClosableIterator<HoodieRecord<R>> getRecordIterator() throws IOException {
    return HoodieFileReader.super.getRecordIterator();
  }

  @Override
  public Schema getSchema() {
    return parquetUtils.readAvroSchema(conf, path);
  }

  @Override
  public void close() {
    readerIterators.forEach(ParquetReaderIterator::close);
  }

  @Override
  public long getTotalRecords() {
    return parquetUtils.getRowCount(conf, path);
  }
}
