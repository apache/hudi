package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.GPubsubMessageReceiver;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.concurrent.TimeUnit;

public class GcsEventsSource extends RowSource {

    private static final Logger LOG = LogManager.getLogger(GcsEventsSource.class);

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
        StreamingContext ssc = new StreamingContext(sparkContext.sc(), Durations.minutes(1));
        JavaStreamingContext jsc = new JavaStreamingContext(ssc);

        GPubsubMessageReceiver receiver = new GPubsubMessageReceiver(
                "redacted","redacted", "redacted");

        JavaDStream<String> istream = jsc.receiverStream(receiver);
        istream.foreachRDD(rdd -> rdd.foreach(x -> System.out.println("output: " + x)));

        try {
            jsc.start();
            try {
                jsc.awaitTerminationOrTimeout(TimeUnit.SECONDS.toMillis(1000L));
            } catch (InterruptedException e) {
                LOG.info("Error", e);
            }
        } finally {
            jsc.stop(true, true);
        }
        return Pair.of(Option.empty(), "Empty data");
    }

    @Override
    public void onCommit(String lastCkptStr) {

    }
}
