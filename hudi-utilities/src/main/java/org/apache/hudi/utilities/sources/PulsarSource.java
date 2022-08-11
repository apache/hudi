package org.apache.hudi.utilities.sources;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.HoodieConversionUtils;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.pulsar.client.api.MessageId;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.pulsar.JsonUtils;

import java.util.Arrays;
import java.util.Collections;


public class PulsarSource extends RowSource {

  private static final String[] PULSAR_META_FIELDS = new String[] {
      "__key",
      "__topic",
      "__messageId",
      "__publishTime",
      "__eventTime",
      "__messageProperties"
  };

  private final String topicName;
  private final String endpointURL;

  public PulsarSource(TypedProperties props,
                      JavaSparkContext sparkContext,
                      SparkSession sparkSession,
                      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);

    DataSourceUtils.checkRequiredProperties(props,
        Arrays.asList(
            Config.PULSAR_SOURCE_TOPIC_NAME.key(),
            Config.PULSAR_SOURCE_ENDPOINT_URL.key()));

    this.topicName = props.getString(Config.PULSAR_SOURCE_TOPIC_NAME.key());
    // TODO validate endpoint provided in the appropriate format
    this.endpointURL = props.getString(Config.PULSAR_SOURCE_ENDPOINT_URL.key());
  }

  @Override
  protected Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCheckpointStr, long sourceLimit) {
    Pair<MessageId, MessageId> startingEndingOffsetsPair = computeOffsets(lastCheckpointStr, sourceLimit);
    MessageId startingOffset = MessageId.earliest;
    MessageId endingOffset = MessageId.latest;

    String startingOffsets = convertToOffsetString(topicName, startingOffset);
    String endingOffsets = convertToOffsetString(topicName, endingOffset);

    //
    // TODO
    //    - Handle checkpoints/offsets
    //      - From --checkpoint param
    //      - From persistence?
    //

    Dataset<Row> sourceRows = sparkSession.read()
        .format("pulsar")
        .option("service.url", endpointURL)
        .option("topics", topicName)
        .option("startingOffsets", startingOffsets)
        .option("endingOffsets", endingOffsets)
        .load();

    return Pair.of(Option.of(transform(sourceRows)), endingOffsets);
  }

  private Dataset<Row> transform(Dataset<Row> rows) {
   return rows.drop(PULSAR_META_FIELDS);
  }

  private Pair<MessageId, MessageId> computeOffsets(Option<String> lastCheckpointStrOpt, long sourceLimit) {
    MessageId startingOffset = fetchStartingOffset(lastCheckpointStrOpt);

    MessageId endingOffset;
    Long maxRecordsLimit = computeTargetRecordLimit(sourceLimit, props);

    return null;
  }

  private MessageId fetchStartingOffset(Option<String> lastCheckpointStrOpt) {
    return lastCheckpointStrOpt.map(lastCheckpoint -> {
      lastCheckpoint
    })
        .orElseGet(() -> {
          Config.OffsetAutoResetStrategy autoResetStrategy = Config.OffsetAutoResetStrategy.valueOf(
              props.getString(Config.PULSAR_SOURCE_OFFSET_AUTO_RESET_STRATEGY.key(),
                  Config.PULSAR_SOURCE_OFFSET_AUTO_RESET_STRATEGY.defaultValue().name()));

          switch (autoResetStrategy) {
            case LATEST:
              return MessageId.latest;
            case EARLIEST:
              return MessageId.earliest;
            case FAIL:
              throw new IllegalArgumentException("No checkpoint has been provided!");
            default:
              throw new UnsupportedOperationException("Unsupported offset auto-reset strategy");
          }
        });
  }

  private static Long computeTargetRecordLimit(long sourceLimit, TypedProperties props) {
    if (sourceLimit < Long.MAX_VALUE) {
      return sourceLimit;
    } else {
      return props.getLong(Config.PULSAR_SOURCE_MAX_RECORDS_PER_BATCH_THRESHOLD_PROP.key(),
          Config.PULSAR_SOURCE_MAX_RECORDS_PER_BATCH_THRESHOLD_PROP.defaultValue());
    }
  }

  private static String convertToOffsetString(String topic, MessageId startingOffset) {
    return JsonUtils.topicOffsets(HoodieConversionUtils.mapAsScalaImmutableMap(Collections.singletonMap(topic, startingOffset)));
  }

  // TODO unify w/ Kafka
  public static class Config {
    private static final ConfigProperty<String> PULSAR_SOURCE_TOPIC_NAME = ConfigProperty
        .key("hoodie.deltastreamer.source.pulsar.topic")
        .noDefaultValue()
        .withDocumentation("Name of the target Pulsar topic to source data from");

    private static final ConfigProperty<String> PULSAR_SOURCE_ENDPOINT_URL = ConfigProperty
        .key("hoodie.deltastreamer.source.pulsar.endpointUrl")
        .noDefaultValue()
        .withDocumentation("URL of the target Pulsar endpoint (of the form 'pulsar://host:port'");

    public enum OffsetAutoResetStrategy {
      LATEST, EARLIEST, FAIL
    }

    private static final ConfigProperty<OffsetAutoResetStrategy> PULSAR_SOURCE_OFFSET_AUTO_RESET_STRATEGY = ConfigProperty
        .key("hoodie.deltastreamer.source.pulsar.offset.autoResetStrategy")
        .defaultValue(OffsetAutoResetStrategy.LATEST)
        .withDocumentation("Policy determining how offsets shall be automatically reset in case there's " +
            "no checkpoint information present");

    public static final ConfigProperty<Long> PULSAR_SOURCE_MAX_RECORDS_PER_BATCH_THRESHOLD_PROP = ConfigProperty
        .key("hoodie.deltastreamer.source.pulsar.maxRecords")
        .defaultValue(10_000_000L)
        .withDocumentation("Max number of records obtained in a single each batch");
  }
}
