/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.sources.pulsar;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieDeltaStreamerException;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.AvroSource;
import org.apache.hudi.utilities.sources.InputBatch;

import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AvroPulsarSource extends AvroSource {

  private final SerializableSchemaInfo schemaInfo;

  private final List<Pair<String, MessageId>> lastMessageIdsByTopicPartition;

  private final TypedProperties pulsarProps;

  public AvroPulsarSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                          SchemaProvider schemaProvider, HoodieDeltaStreamerMetrics metrics) {
    super(props, sparkContext, sparkSession, schemaProvider);
    DataSourceUtils.checkRequiredProperties(props, CollectionUtils.createImmutableList(
        Config.PULSAR_HTTP_URL,
        Config.PULSAR_TOPIC_NAME,
        Config.PULSAR_SERVICE_URL
    ));

    try {
      this.pulsarProps = props;
      String topic = props.getProperty(Config.PULSAR_TOPIC_NAME);
      PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(props.getProperty(Config.PULSAR_HTTP_URL)).build();
      this.schemaInfo = new SerializableSchemaInfo(admin.schemas().getSchemaInfo(TopicName.get(topic).toString()));
      PulsarUtils.checkSupportedSchema(schemaInfo.get());

      // Fetch the topic metadata and determine parallel tasks needed
      PartitionedTopicMetadata topicMeta = admin.topics().getPartitionedTopicMetadata(topic);
      this.lastMessageIdsByTopicPartition = new ArrayList<>();
      if (topicMeta.partitions == 0) {
        lastMessageIdsByTopicPartition.add(Pair.of(topic, admin.topics().getLastMessageId(topic)));
      } else {
        for (int p = 0; p < topicMeta.partitions; p++) {
          String topicPartition = String.format("%s-partition-%d", topic, p);
          lastMessageIdsByTopicPartition.add(Pair.of(topicPartition, admin.topics().getLastMessageId(topicPartition)));
        }
      }

      admin.close();
    } catch (Exception e) {
      throw new HoodieDeltaStreamerException("Unable to initialize the pulsar source", e);
    }
  }

  static class Config {
    static final String PULSAR_CONF_PREFIX = "hoodie.deltastreamer.source.pulsar";
    static final String PULSAR_SERVICE_URL = PULSAR_CONF_PREFIX + ".client.serviceUrl";
    static final String PULSAR_TOPIC_NAME = PULSAR_CONF_PREFIX + ".reader.topicName";
    static final String PULSAR_HTTP_URL = PULSAR_CONF_PREFIX + ".httpUrl";

    static Map<String, Object> getConf(String prefix, TypedProperties props) {
      Map<String, Object> conf = new HashMap<>();
      props.forEach((k, v) -> {
        if (k.toString().startsWith(prefix)) {
          conf.put(k.toString().replaceAll(prefix, ""), v);
        }
      });
      return conf;
    }
  }

  @Override
  protected InputBatch<JavaRDD<GenericRecord>> fetchNewData(Option<String> lastCkptStr, long sourceLimit) {

    try {
      final Map<String, MessageId> startingOffsets = new HashMap<>();
      lastMessageIdsByTopicPartition.forEach(p -> startingOffsets.put(p.getKey(), MessageId.earliest));
      if (lastCkptStr.isPresent()) {
        PulsarUtils.strToOffsets(lastCkptStr.get()).forEach(startingOffsets::put);
      }
      System.err.println(">>> Starting Offsets" + startingOffsets);

      // In parallel, fetch from each topic partition upto the end offsets.
      JavaRDD<GenericRecord> avroRDD = sparkContext.parallelize(lastMessageIdsByTopicPartition, lastMessageIdsByTopicPartition.size())
          .mapPartitions(itr -> new Iterator<GenericRecord>() {
            private PulsarClient client;
            private Reader reader;
            private SchemaInfo info = schemaInfo.get();
            private boolean reachedEndOffset;
            private final Pair<String, MessageId> tpLastOffset = itr.next();

            private void lazyInit() throws PulsarClientException {
              if (this.client == null) {
                String serviceUrl = pulsarProps.getString(Config.PULSAR_SERVICE_URL);
                this.client = PulsarClient.builder()
                    .serviceUrl(serviceUrl)
                    .loadConf(Config.getConf(Config.PULSAR_CONF_PREFIX + "client.", pulsarProps))
                    .build();
                this.reader = this.client
                    .newReader(Schema.getSchema(info))
                    .loadConf(Config.getConf(Config.PULSAR_CONF_PREFIX + "reader.", pulsarProps))
                    .topic(tpLastOffset.getKey())
                    .startMessageId(startingOffsets.get(tpLastOffset.getKey()))
                    .create();
              }
            }

            @Override
            public boolean hasNext() {
              try {
                lazyInit();
                boolean ret = reader.hasMessageAvailable() && !reachedEndOffset;
                if (!ret) {
                  this.reader.close();
                  this.client.close();
                }
                return ret;
              } catch (IOException e) {
                throw new HoodieDeltaStreamerException("Error fetching from Pulsar", e);
              }
            }

            @Override
            public GenericRecord next() {
              try {
                Message m = reader.readNext();
                // we need to stop once we reach the end-offset
                if (m.getMessageId().compareTo(tpLastOffset.getRight()) == 0) {
                  reachedEndOffset = true;
                } else if (m.getMessageId().compareTo(tpLastOffset.getRight()) > 0) {
                  throw new HoodieDeltaStreamerException("Encountered a later message at " + m.getMessageId() + " while "
                      + "still waiting for end message" + tpLastOffset.getRight());
                }
                return PulsarUtils.unshadedRecord(PulsarUtils.extractValue(m, info).getAvroRecord());
              } catch (PulsarClientException e) {
                throw new HoodieDeltaStreamerException("Error fetching from Pulsar", e);
              }
            }
          });


      // TODO: need to return a schema provider that can hand the source Pulsar schema.
      Map<String, MessageId> endOffsets = lastMessageIdsByTopicPartition.stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue));
      return new InputBatch<>(Option.of(avroRDD), PulsarUtils.offsetsToStr(endOffsets));
    } catch (Exception e) {
      throw new HoodieDeltaStreamerException("Unable to fetch new pulsar data from checkpoint " + lastCkptStr, e);
    }
  }

  public static void main(String[] args) throws Exception {
    SparkSession sparkSession = SparkSession.builder()
        .master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate();

    TypedProperties properties = new TypedProperties();
    properties.setProperty(Config.PULSAR_SERVICE_URL, "pulsar://localhost:6650");
    properties.setProperty(Config.PULSAR_HTTP_URL, "http://localhost:8080");
    properties.setProperty(Config.PULSAR_TOPIC_NAME, "public/default/dbserver1.inventory.products");
    JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());


    Option<String> checkpoint = Option.empty();
    /*String chkpt =
        "public/default/person_topic-partition-3:08f20f1031180320003000,public/default/person_topic-partition-0:08f30f1030180020003000,
        public/default/person_topic-partition-1:08f40f1031180120003000,
        public/default/person_topic-partition-2:08f50f102e180220003000";

    checkpoint = Option.of(chkpt);*/
    AvroPulsarSource pulsarSource = new AvroPulsarSource(properties, jsc, sparkSession, null, null);
    InputBatch<JavaRDD<GenericRecord>> b = pulsarSource.fetchNewData(checkpoint, Long.MAX_VALUE);
    System.err.println(">>> " + b.getBatch().get().count());
    System.err.println(">>> " + b.getCheckpointForNextBatch());
  }
}
