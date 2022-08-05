package org.apache.hudi.utilities.sources;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.HoodieConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.pulsar.JsonUtils;
import scala.collection.JavaConverters;
import scala.collection.convert.AsScalaConverters;

import java.util.Collections;


public class PulsarSource extends RowSource {
  public PulsarSource(TypedProperties props,
                      JavaSparkContext sparkContext,
                      SparkSession sparkSession,
                      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
  }

  @Override
  protected Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {
    String topic = "test-topic";
    MessageId startingOffset = MessageId.earliest;
    MessageId endingOffset = MessageId.latest;

    String startingOffsets = convertToOffsetString(topic, startingOffset);
    String endingOffsets = convertToOffsetString(topic, endingOffset);

    Dataset<Row> rows = sparkSession.read()
        .format("pulsar")
        .option("service.url", "pulsar://localhost:6650")
        .option("topics", "topic1,topic2")
        .option("startingOffsets", startingOffsets)
        .option("endingOffsets", endingOffsets)
        .load();

    return Pair.of(Option.of(rows), endingOffsets);
  }

  private static String convertToOffsetString(String topic, MessageId startingOffset) {
    return JsonUtils.topicOffsets(HoodieConversionUtils.mapAsScalaImmutableMap(Collections.singletonMap(topic, startingOffset)));
  }
}
