package org.apache.hudi.utilities.sources;

import com.fasterxml.jackson.databind.ObjectMapper;
//import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
//import com.google.api.client.googleapis.json.GoogleJsonResponseException;
//import com.google.api.client.googleapis.util.Utils;
//import com.google.api.client.http.HttpTransport;
//import com.google.api.client.json.JsonFactory;

/** TODO: See if api-services-pubsub is needed at all
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.api.services.pubsub.model.Subscription;
*/
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
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class GcsEventsSource extends RowSource {

    private static final Logger LOG = LogManager.getLogger(GcsEventsSource.class);

    private final int BATCH_SIZE = 10;

    public GcsEventsSource(
            TypedProperties props,
            JavaSparkContext sparkContext,
            SparkSession sparkSession,
            SchemaProvider schemaProvider) {
        super(props, sparkContext, sparkSession, schemaProvider);
        LOG.info("Creating GcsEventsSource");
    }

    @Override
    protected Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {
        LOG.info("Fetching next batch");

//        Pair<List<String>, String> messagesAndMaxTime = pull("redacted", "redacted",
//                "redacted");

        Pair<List<String>, String> messagesAndMaxTime = pull2("redacted", "redacted");

        Dataset<String> eventRecords = sparkSession.createDataset(messagesAndMaxTime.getLeft(), Encoders.STRING());

        return Pair.of(
            Option.of(sparkSession.read().json(eventRecords)),
            messagesAndMaxTime.getRight()
        );
    }

    public Pair<List<String>, String> pull2(String projectId, String subscriptionId) {
        SubscriberStubSettings subscriberStubSettings = null;

        try {
            subscriberStubSettings = SubscriberStubSettings.newBuilder()
                    .setTransportChannelProvider(
                            SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                                    .setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
                                    .build())
                    .build();

            try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {
                String subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);
                PullRequest pullRequest =
                        PullRequest.newBuilder()
                                .setMaxMessages(BATCH_SIZE)
                                .setSubscription(subscriptionName)
                                .build();

                // Use pullCallable().futureCall to asynchronously perform this operation.
                PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);

                // Stop the program if the pull response is empty to avoid acknowledging
                // an empty list of ack IDs.
                List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessagesList();

                if (receivedMessages.isEmpty()) {
                    System.out.println("No message was pulled. Exiting.");
                    return null;
                }

//                ackMessages(subscriber, subscriptionName, receivedMessages);

                return processMessages(receivedMessages);
            }
        } catch (IOException e) {
            LOG.error("Error", e);
        }

        return Pair.of(Collections.emptyList(), "0");
    }

    private void ackMessages(SubscriberStub subscriber, String subscriptionName,
                             List<ReceivedMessage> receivedMessages) {
            List<String> ackIds = new ArrayList<>();
            for (ReceivedMessage message : receivedMessages) {
                ackIds.add(message.getAckId());
            }

            AcknowledgeRequest acknowledgeRequest =
                    AcknowledgeRequest.newBuilder()
                            .setSubscription(subscriptionName)
                            .addAllAckIds(ackIds)
                            .build();

            // Use acknowledgeCallable().futureCall to asynchronously perform this operation.
            subscriber.acknowledgeCallable().call(acknowledgeRequest);
    }


//    public Pair<List<String>, String> pull(String projectName, String topicName, String subscriptionName) {
//        String projectFullName = "projects/" + projectName;
//        String topicFullName = projectFullName + "/topics/" + topicName;
//        String subscriptionFullName = projectFullName + "/subscriptions/" + subscriptionName;
//
//        Pubsub client = createAuthorizedClient();
//
//        try {
//            Subscription subscription = new Subscription().setTopic(topicFullName);
//            try {
//                subscription =
//                        client.projects().subscriptions().create(subscriptionFullName, subscription).execute();
//            } catch (GoogleJsonResponseException e) {
//                if (e.getDetails().getCode() == HttpURLConnection.HTTP_CONFLICT) {
//                    // Subscription already exists, but that's the expected behavior with multiple receivers.
//                } else {
//                   throw e;
//                }
//            }
//
//            // TODO: Timeouts etc
//            PullRequest pullRequest =
//                new PullRequest().setReturnImmediately(false).setMaxMessages(BATCH_SIZE);
//
//            PullResponse pullResponse =
//                client.projects().subscriptions().pull(subscriptionFullName, pullRequest).execute();
//
//            List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessages();
//            if (receivedMessages == null) {
//                return Pair.of(Collections.emptyList(), "0");
//            }
//
//            return processMessages(receivedMessages);
//
//        } catch (GoogleJsonResponseException e) {
//            // TODO: Check if this catch block is needed
//            LOG.error("Error", e);
//        } catch (IOException e) {
//            LOG.error("Error", e);
//        }
//
//        return Pair.of(Collections.emptyList(), "0");
//    }

    private Pair<List<String>, String> processMessages(List<ReceivedMessage> receivedMessages) throws IOException {
        List<String> messages = mapToString(receivedMessages);
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> messageMaps = new ArrayList<>();
        for (String msg : messages) {
            LOG.info("msg: " + msg);
            messageMaps.add((Map<String, Object>) mapper.readValue(msg, Map.class));
        }

        String maxTime = new Long(messageMaps.stream().mapToLong(m -> Date.from(Instant.from(
                DateTimeFormatter.ISO_INSTANT.parse((String) m.get("timeCreated"))))
                .getTime()).max().orElse(0L)).toString();

        return Pair.of(messages, maxTime);
    }

    private List<String> mapToString(List<ReceivedMessage> messages) {
        List<String> msgTxt = messages
                .stream()
                .filter(m -> m.getMessage() != null)
                .filter(m -> m.getMessage().getData() != null)
                .map(
                    m -> {
//                        try {
                            return m.getMessage().getData().toStringUtf8();
//                        } catch (UnsupportedEncodingException e) {
//                            // Wrong encode
//                            return null;
//                        }
                    })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        return msgTxt;
    }

    // TODO: Fix hardcoded auths
//    private Pubsub createAuthorizedClient() {
//        try {
//            HttpTransport httpTransport = Utils.getDefaultTransport();
//            JsonFactory jsonFactory = Utils.getDefaultJsonFactory();
//            GoogleCredential creds =
//                    GoogleCredential.getApplicationDefault(httpTransport, jsonFactory);
//
//            return new Pubsub.Builder(httpTransport, jsonFactory, req -> req.setInterceptor(creds)).
//                    setApplicationName("spark-pubsub-receiver").
//                    setHttpRequestInitializer(req -> req.setInterceptor(creds))
//                    .build();
//
//        } catch (IOException e) {
//            LOG.error("Error", e);
//        }
//
//        return null;
//    }
}
