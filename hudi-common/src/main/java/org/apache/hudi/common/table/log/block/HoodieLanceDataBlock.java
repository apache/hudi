package org.apache.hudi.common.table.log.block;

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.inline.InLineFSUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.model.HoodieFileFormat.LANCE;
import static org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER;

public class HoodieLanceDataBlock extends HoodieDataBlock {
  public HoodieLanceDataBlock(List<HoodieRecord> records, Map<HeaderMetadataType, String> header, Map<FooterMetadataType, String> footer, String keyFieldName) {
    super(records, header, footer, keyFieldName);
  }

  @Override
  protected ByteArrayOutputStream serializeRecords(List<HoodieRecord> records, HoodieStorage storage) throws IOException {
    HoodieSchema writerSchema = HoodieSchemaCache.intern(HoodieSchema.parse(super.getLogBlockHeader().get(HoodieLogBlock.HeaderMetadataType.SCHEMA)));
    return HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(LANCE)
        .serializeRecordsToLogBlock(storage, records, writerSchema, getSchema(), getKeyFieldName(), Collections.emptyMap());
  }

  /**
   * NOTE: We're overriding the whole reading sequence to make sure we properly respect
   *       the requested Reader's schema and only fetch the columns that have been explicitly
   *       requested by the caller (providing projected Reader's schema)
   */
  @Override
  protected <T> ClosableIterator<HoodieRecord<T>> readRecordsFromBlockPayload(HoodieRecord.HoodieRecordType type) throws IOException {
    HoodieLogBlockContentLocation blockContentLoc = getBlockContentLocation().get();

    // NOTE: It's important to extend Hadoop configuration here to make sure configuration
    //       is appropriately carried over
    StorageConfiguration<?> inlineConf = blockContentLoc.getStorage().getConf().getInline();
    StoragePath inlineLogFilePath = InLineFSUtils.getInlineFilePath(
        blockContentLoc.getLogFile().getPath(),
        blockContentLoc.getLogFile().getPath().toUri().getScheme(),
        blockContentLoc.getContentPositionInLogFile(),
        blockContentLoc.getBlockSize());

    HoodieStorage inlineStorage = getBlockContentLocation().get().getStorage().newInstance(inlineLogFilePath, inlineConf);
    HoodieSchema writerSchema = HoodieSchema.parse(this.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));

    return HoodieIOFactory.getIOFactory(inlineStorage)
        .getReaderFactory(type)
        .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, inlineLogFilePath, LANCE, Option.empty())
        .getRecordIterator(writerSchema, readerSchema);
  }

  @Override
  protected <T> ClosableIterator<T> readRecordsFromBlockPayload(HoodieReaderContext<T> readerContext) throws IOException {
    HoodieLogBlockContentLocation blockContentLoc = getBlockContentLocation().get();

    // NOTE: It's important to extend Hadoop configuration here to make sure configuration
    //       is appropriately carried over
    StorageConfiguration<?> inlineConf = blockContentLoc.getStorage().getConf().getInline();

    StoragePath inlineLogFilePath = InLineFSUtils.getInlineFilePath(
        blockContentLoc.getLogFile().getPath(),
        blockContentLoc.getLogFile().getPath().toUri().getScheme(),
        blockContentLoc.getContentPositionInLogFile(),
        blockContentLoc.getBlockSize());
    HoodieStorage inlineStorage = blockContentLoc.getStorage().newInstance(inlineLogFilePath, inlineConf);

    HoodieSchema writerSchema = HoodieSchema.parse(this.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));

    return readerContext.getFileRecordIterator(
        inlineLogFilePath, 0, blockContentLoc.getBlockSize(),
        writerSchema,
        readerSchema,
        inlineStorage);
  }

  @Override
  protected <T> ClosableIterator<HoodieRecord<T>> deserializeRecords(byte[] content, HoodieRecord.HoodieRecordType type) throws IOException {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  /**
   * Streaming deserialization of records.
   *
   * @param inputStream The input stream from which to read the records.
   * @param contentLocation The location within the input stream where the content starts.
   * @param bufferSize The size of the buffer to use for reading the records.
   * @return A ClosableIterator over HoodieRecord<T>.
   * @throws IOException If there is an error reading or deserializing the records.
   */
  protected <T> ClosableIterator<HoodieRecord<T>> deserializeRecords(
      SeekableDataInputStream inputStream,
      HoodieLogBlockContentLocation contentLocation,
      HoodieRecord.HoodieRecordType type,
      int bufferSize
  ) throws IOException {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  protected <T> ClosableIterator<T> deserializeRecords(HoodieReaderContext<T> readerContext, byte[] content) throws IOException {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.LANCE_DATA_BLOCK;
  }
}
