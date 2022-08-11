//package org.apache.hudi.utilities.sources.helpers;
//
//import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
//import com.google.api.client.googleapis.json.GoogleJsonResponseException;
//import com.google.api.client.googleapis.util.Utils;
//import com.google.api.client.http.HttpTransport;
//import com.google.api.client.json.JsonFactory;
//import com.google.api.services.pubsub.Pubsub;
//import com.google.api.services.pubsub.model.PullRequest;
//import com.google.api.services.pubsub.model.PullResponse;
//import com.google.api.services.pubsub.model.ReceivedMessage;
//import com.google.api.services.pubsub.model.Subscription;
//import org.apache.spark.storage.StorageLevel;
//import org.apache.spark.streaming.receiver.Receiver;
//
//import java.io.IOException;
//import java.io.UnsupportedEncodingException;
//import java.net.HttpURLConnection;
//import java.util.List;
//
///**
// * Implements Spark Streaming's Receiver interface. Can be used within a Spark Streaming
// * job to receive messages from a Cloud Pubsub topic
// *
// * TODO: Can be removed if GcsEventsSource can be made to work without using
// * Spark's StreamingContext
// */
//public class GPubsubMessageReceiver extends Receiver<String> {
//
//    private final int BATCH_SIZE = 2;
//
//    private final String projectFullName;
//    private final String topicFullName;
//    private final String subscriptionFullName;
//
//    public GPubsubMessageReceiver(String projectName, String topicName, String subscription) {
//        super(StorageLevel.MEMORY_AND_DISK_2());
//        this.projectFullName = "projects/" + projectName;
//        this.topicFullName = projectFullName + "/topics/" + topicName;
//        this.subscriptionFullName = projectFullName + "/subscriptions/" + subscription;
//    }
//
//    public void onStart() {
//        Pubsub client = createAuthorizedClient();
//        Subscription subscription = new Subscription().setTopic(topicFullName);
//        try {
//            subscription =
//                    client.projects().subscriptions().create(subscriptionFullName, subscription).execute();
//        } catch (GoogleJsonResponseException e) {
//            if (e.getDetails().getCode() == HttpURLConnection.HTTP_CONFLICT) {
//                // Subscription already exists, but that's the expected behavior with multiple receivers.
//            } else {
//                e.printStackTrace();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        new Thread() {
//            @Override
//            public void run() {
//                receive();
//            }
//        }.start();
//    }
//
//    public void onStop() {
//        System.out.println("Stopping GPubsubMessageReceiver");
//    }
//
//    // Pull messages from Pubsub
//    private void receive() {
//        Pubsub client = createAuthorizedClient();
//        PullRequest pullRequest =
//                new PullRequest().setReturnImmediately(false).setMaxMessages(BATCH_SIZE);
//        do {
//            try {
//                PullResponse pullResponse =
//                        client.projects().subscriptions().pull(subscriptionFullName, pullRequest).execute();
//
//                List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessages();
//                if (receivedMessages != null) {
//                    storeMessages(receivedMessages);
//                }
//            } catch (GoogleJsonResponseException e) {
//                e.printStackTrace();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        } while (!isStopped());
//    }
//
//    private void storeMessages(List<ReceivedMessage> messages) {
//        store(
//            messages
//            .stream()
//            .filter(m -> m.getMessage() != null)
//            .filter(m -> m.getMessage().decodeData() != null)
//            .map(
//                m -> {
//                    try {
//                        String s = new String(m.getMessage().decodeData(), "UTF-8");
//                        return s;
//                    } catch (UnsupportedEncodingException e) {
//                        // Wrong encode
//                        return null;
//                    }
//                })
//            .filter(m -> m != null)
//            .iterator()
//        );
//    }
//
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
//            .build();
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        return null;
//    }
//
//}
