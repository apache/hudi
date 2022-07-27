package org.apache.hudi.common.testutils;

import org.apache.hudi.avro.MercifulJsonConverter;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Map;

/**
 * Test payload for S3 event here (https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html).
 */
public class S3EventTestPayload extends GenericTestPayload implements HoodieRecordPayload<S3EventTestPayload> {
  public S3EventTestPayload(Option<String> jsonData, String rowKey, String partitionPath, String schemaStr,
                            Boolean isDeleted, Comparable orderingVal) throws IOException {
    super(jsonData, rowKey, partitionPath, schemaStr, isDeleted, orderingVal);
  }

  public S3EventTestPayload(String jsonData, String rowKey, String partitionPath, String schemaStr) throws IOException {
    this(Option.of(jsonData), rowKey, partitionPath, schemaStr, false, 0L);
  }

  public S3EventTestPayload(String jsonData) throws IOException {
    super(jsonData);
  }

  public S3EventTestPayload preCombine(S3EventTestPayload oldValue) {
    throw new UnsupportedOperationException("preCombine not implemented for S3EventTestPayload");
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord oldRec, Schema schema) throws IOException {
    throw new UnsupportedOperationException("combineAndGetUpdateValue not implemented for S3EventTestPayload");
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    if (isDeleted) {
      return Option.empty();
    } else {
      MercifulJsonConverter jsonConverter = new MercifulJsonConverter();
      return Option.of(jsonConverter.convert(getJsonData(), schema));
    }
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    return Option.empty();
  }
}
