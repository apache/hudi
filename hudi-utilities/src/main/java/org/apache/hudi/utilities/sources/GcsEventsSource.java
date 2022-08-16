package org.apache.hudi.utilities.sources;

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/*
 * Launch in spark-submit as follows:
 *
 bin/spark-submit \
--packages org.apache.spark:spark-avro_2.11:2.4.4 \
--packages com.google.api:gax-grpc:2.18.2 \
--packages com.google.cloud:google-cloud-pubsub:1.120.0 \
--driver-memory 4g \
--executor-memory 4g \
--class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
<path_to_hudi>/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.11.0-SNAPSHOT.jar \
--hoodie-conf hoodie.datasource.write.recordkey.field="id,metageneration" \
--hoodie-conf hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.ComplexKeyGenerator \
--hoodie-conf hoodie.datasource.write.partitionpath.field=bucket \
--source-class org.apache.hudi.utilities.sources.GcsEventsSource \
--table-type COPY_ON_WRITE \
--source-ordering-field timeCreated \
--target-base-path file:\/\/\/home/pramod/2tmp/dl-try/gcs-data \
--target-table gcs_tbl \
--op UPSERT \
--continuous \
--source-limit 100 \
--min-sync-interval-seconds 180
 * Needs spark-defaults.conf to look like this:
spark.driver.extraClassPath <path_to_maven_repo_dir>/com/google/protobuf/protobuf-java/3.21.1/protobuf-java-3.21.1.jar:<path_to_maven_repo_dir>/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar:<path_to_maven_repo_dir>/com/google/guava/guava/31.1-jre/guava-31.1-jre.jar
spark.executor.extraClassPath <path_to_maven_repo_dir>/com/google/protobuf/protobuf-java/3.21.1/protobuf-java-3.21.1.jar:<path_to_maven_repo_dir>/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar:<path_to_maven_repo_dir>/com/google/guava/guava/31.1-jre/guava-31.1-jre.jar
 */
public class GcsEventsSource extends RowSource {

    private final String googleProjectId;
    private final String pubsubSubscriptionId;

    private SubscriberStubSettings subscriberStubSettings;
    private final List<String> messagesToAck = new ArrayList<>();

    private static final int BATCH_SIZE = 100;

    private static final Logger LOG = LogManager.getLogger(GcsEventsSource.class);

    public GcsEventsSource(
            TypedProperties props,
            JavaSparkContext sparkContext,
            SparkSession sparkSession,
            SchemaProvider schemaProvider) {
        super(props, sparkContext, sparkSession, schemaProvider);
        try {
            subscriberStubSettings = SubscriberStubSettings.newBuilder()
                .setTransportChannelProvider(
                    SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                        .setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
                        .build())
                .build();
        } catch (IOException e) {
            throw new HoodieException("Error creating subscriber stub settings", e);
        }

        this.googleProjectId = "redacted";
        this.pubsubSubscriptionId = "redacted";

        LOG.info("Created GcsEventsSource");
    }

    @Override
    protected Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {
        LOG.info("Fetching next batch");

        Pair<List<String>, String> messagesAndMaxTime = fetchMetadata(lastCkptStr);

        Dataset<String> eventRecords = sparkSession.createDataset(messagesAndMaxTime.getLeft(), Encoders.STRING());

        return Pair.of(
            Option.of(sparkSession.read().json(eventRecords)),
            messagesAndMaxTime.getRight()
        );
    }

    @Override
    public void onCommit(String lastCkptStr) {
        LOG.info("GcsEventsSource.onCommit()");
        ackOutstandingMessages();
    }

    public Pair<List<String>, String> fetchMetadata(Option<String> lastCheckpoint) {
        try {
            try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {
                String subscriptionName = ProjectSubscriptionName.format(googleProjectId, pubsubSubscriptionId);

                PullRequest pullRequest = PullRequest.newBuilder()
                    .setMaxMessages(BATCH_SIZE)
                    .setSubscription(subscriptionName)
                    .build();

                // TODO: Timeout and retry
                PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);

                List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessagesList();

                if (receivedMessages.isEmpty()) {
                    System.out.println("No message was pulled. Exiting.");
                    return null;
                }

                return processMessages(receivedMessages, lastCheckpoint);
            }
        } catch (IOException e) {
            LOG.error("Error", e);
        }

        return Pair.of(Collections.emptyList(), "0");
    }

    private Pair<List<String>, String> processMessages(List<ReceivedMessage> receivedMessages,
                                                       Option<String> lastCheckpoint) throws IOException {
        List<String> messages = new ArrayList<>();

        for (ReceivedMessage message: receivedMessages) {
            String msgStr = message.getMessage().getData().toStringUtf8();
            LOG.info("msg: " + msgStr);

            messages.add(msgStr);
            messagesToAck.add(message.getAckId());
        }

        String maxTime = new Long(receivedMessages.stream().mapToLong(m -> Date.from(Instant.from(
                DateTimeFormatter.ISO_INSTANT.parse((String) m.getMessage().getAttributesMap().get("eventTime"))))
                .getTime()).max().orElse(lastCheckpoint.map(Long::parseLong).orElse(0L))).toString();

        return Pair.of(messages, maxTime);
    }

    private void ackOutstandingMessages() {
        try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {
            String subscriptionName = ProjectSubscriptionName.format(googleProjectId, pubsubSubscriptionId);

            AcknowledgeRequest acknowledgeRequest = AcknowledgeRequest.newBuilder()
                .setSubscription(subscriptionName)
                .addAllAckIds(messagesToAck)
                .build();

            // TODO: Timeout and retry
            subscriber.acknowledgeCallable().call(acknowledgeRequest);
            LOG.info("Acknowledged messages: " + messagesToAck);
        } catch (IOException e) {
            throw new HoodieException("Error when acknowledging messages from Pubsub", e);
        }
    }
}
