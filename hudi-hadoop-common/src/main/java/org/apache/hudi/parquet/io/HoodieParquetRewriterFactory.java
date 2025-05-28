package org.apache.hudi.parquet.io;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.io.storage.rewrite.HoodieFileMetadataMerger;
import org.apache.hudi.io.storage.rewrite.HoodieFileRewriter;
import org.apache.hudi.io.storage.rewrite.HoodieFileRewriterFactory;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;

public class HoodieParquetRewriterFactory extends HoodieFileRewriterFactory {

  @Override
  protected <T> HoodieFileRewriter newFileRewriter(
      List<StoragePath> inputFilePaths,
      StoragePath targetFilePath,
      Configuration conf,
      HoodieConfig config,
      HoodieFileMetadataMerger metadataMerger,
      Schema writeSchemaWithMetaFields) throws IOException {

    ValidationUtils.checkArgument(writeSchemaWithMetaFields != null,
        "write schema for ParquetFileRewriter can not be null");
    MessageType writeSchema = new AvroSchemaConverter(conf).convert(writeSchemaWithMetaFields);
    String compressionCodecName = config.getStringOrDefault(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME);
    return new HoodieParquetFileRewriter(
        inputFilePaths,
        targetFilePath,
        conf,
        CompressionCodecName.fromConf(compressionCodecName.isEmpty() ? null : compressionCodecName),
        metadataMerger,
        writeSchema);
  }
}
