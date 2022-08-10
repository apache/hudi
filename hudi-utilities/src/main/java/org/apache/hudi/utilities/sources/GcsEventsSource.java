package org.apache.hudi.utilities.sources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.api.services.pubsub.model.Subscription;
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
import java.util.Optional;
import java.util.stream.Collectors;

public class GcsEventsSource extends RowSource {

    private static final Logger LOG = LogManager.getLogger(GcsEventsSource.class);

    private final int BATCH_SIZE = 2;

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
//        StreamingContext ssc = new StreamingContext(sparkContext.sc(), Durations.minutes(1));
//        JavaStreamingContext jsc = new JavaStreamingContext(ssc);

//        GPubsubMessageReceiver receiver = new GPubsubMessageReceiver(
//                "redacted","redacted", "redacted");
//
//        JavaDStream<String> istream = jsc.receiverStream(receiver);
//        istream.foreachRDD(rdd -> rdd.foreach(x -> System.out.println("output: " + x)));
        Pair<List<String>, String> messagesAndMaxTime = pull("redacted", "redacted", "redacted");

//        try {
//            jsc.start();
//            try {
//                jsc.awaitTerminationOrTimeout(TimeUnit.SECONDS.toMillis(1000L));
//            } catch (InterruptedException e) {
//                LOG.info("Error", e);
//            }
//        } finally {
//            jsc.stop(true, true);
//        }
        Dataset<String> eventRecords = sparkSession.createDataset(messagesAndMaxTime.getLeft(), Encoders.STRING());
        return Pair.of(
            Option.of(sparkSession.read().json(eventRecords)),
            messagesAndMaxTime.getRight()
        );
    }

    public Pair<List<String>, String> pull(String projectName, String topicName, String subscriptionName) {
        String projectFullName = "projects/" + projectName;
        String topicFullName = projectFullName + "/topics/" + topicName;
        String subscriptionFullName = projectFullName + "/subscriptions/" + subscriptionName;

        Pubsub client = createAuthorizedClient();

        try {
            Subscription subscription = new Subscription().setTopic(topicFullName);

            try {
                subscription =
                        client.projects().subscriptions().create(subscriptionFullName, subscription).execute();
            } catch (GoogleJsonResponseException e) {
                if (e.getDetails().getCode() == HttpURLConnection.HTTP_CONFLICT) {
                    // Subscription already exists, but that's the expected behavior with multiple receivers.
                } else {
                   throw e;
                }
            }

            // TODO: Timeouts etc
            PullRequest pullRequest =
                new PullRequest().setReturnImmediately(false).setMaxMessages(BATCH_SIZE);

            PullResponse pullResponse =
                client.projects().subscriptions().pull(subscriptionFullName, pullRequest).execute();

            List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessages();
            if (receivedMessages != null) {
                List<String> messages = extractMessages(receivedMessages);
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
        } catch (GoogleJsonResponseException e) {
            // TODO: Check if this catch block is needed
            LOG.error("Error", e);
        } catch (IOException e) {
            LOG.error("Error", e);
        }

        return Pair.of(Collections.emptyList(), "0");
    }

    public void onStop() {
        System.out.println("Stopping GPubsubMessageReceiver");
    }

    private List<String> extractMessages(List<ReceivedMessage> messages) {
        List<String> msgTxt = messages
                .stream()
                .filter(m -> m.getMessage() != null)
                .filter(m -> m.getMessage().decodeData() != null)
                .map(
                    m -> {
                        try {
                            String s = new String(m.getMessage().decodeData(), "UTF-8");
                            return s;
                        } catch (UnsupportedEncodingException e) {
                            // Wrong encode
                            return null;
                        }
                    })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        return msgTxt;
    }

    private Pubsub createAuthorizedClient() {
        try {
            HttpTransport httpTransport = Utils.getDefaultTransport();
            JsonFactory jsonFactory = Utils.getDefaultJsonFactory();
            GoogleCredential creds =
                    GoogleCredential.getApplicationDefault(httpTransport, jsonFactory);

            return new Pubsub.Builder(httpTransport, jsonFactory, req -> req.setInterceptor(creds)).
                    setApplicationName("spark-pubsub-receiver").
                    setHttpRequestInitializer(req -> req.setInterceptor(creds))
                    .build();

        } catch (IOException e) {
            LOG.error("Error", e);
        }

        return null;
    }
}
