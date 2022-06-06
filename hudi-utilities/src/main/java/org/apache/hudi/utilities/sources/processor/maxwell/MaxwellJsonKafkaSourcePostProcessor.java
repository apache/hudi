/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.sources.processor.maxwell;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.DateTimeUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.exception.HoodieSourcePostProcessException;
import org.apache.hudi.utilities.sources.processor.JsonKafkaSourcePostProcessor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.api.java.JavaRDD;

import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.apache.hudi.utilities.sources.processor.maxwell.PreCombineFieldType.DATE_STRING;
import static org.apache.hudi.utilities.sources.processor.maxwell.PreCombineFieldType.EPOCHMILLISECONDS;
import static org.apache.hudi.utilities.sources.processor.maxwell.PreCombineFieldType.NON_TIMESTAMP;
import static org.apache.hudi.utilities.sources.processor.maxwell.PreCombineFieldType.UNIX_TIMESTAMP;
import static org.apache.hudi.utilities.sources.processor.maxwell.PreCombineFieldType.valueOf;

/**
 * A {@link JsonKafkaSourcePostProcessor} help to extract fresh data from maxwell json string and tag the record as
 * delete or not.
 */
public class MaxwellJsonKafkaSourcePostProcessor extends JsonKafkaSourcePostProcessor {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Option<String> databaseRegex;
  private final String tableRegex;

  public MaxwellJsonKafkaSourcePostProcessor(TypedProperties props) {
    super(props);
    // PRECOMBINE_FIELD_NAME has no default value, but maxwell json string depends on `ts` field
    if (!props.containsKey(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key())) {
      this.props.setProperty(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), TS);
    }
    databaseRegex = Option.ofNullable(props.getString(Config.DATABASE_NAME_REGEX_PROP.key(), null));
    tableRegex = props.getString(Config.TABLE_NAME_REGEX_PROP.key());
  }

  // ------------------------------------------------------------------------
  //  Partial fields in maxwell json string
  // ------------------------------------------------------------------------

  private static final String DATABASE = "database";
  private static final String TABLE = "table";
  private static final String DATA = "data";
  private static final String OPERATION_TYPE = "type";
  private static final String TS = "ts";

  // ------------------------------------------------------------------------
  //  Operation types
  // ------------------------------------------------------------------------

  private static final String INSERT = "insert";
  private static final String UPDATE = "update";
  private static final String DELETE = "delete";

  /**
   * Configs to be passed for this processor.
   */
  public static class Config {
    public static final ConfigProperty<String> DATABASE_NAME_REGEX_PROP = ConfigProperty
        .key("hoodie.deltastreamer.source.json.kafka.post.processor.maxwell.database.regex")
        .noDefaultValue()
        .withDocumentation("Database name regex.");

    public static final ConfigProperty<String> TABLE_NAME_REGEX_PROP = ConfigProperty
        .key("hoodie.deltastreamer.source.json.kafka.post.processor.maxwell.table.regex")
        .noDefaultValue()
        .withDocumentation("Table name regex.");

    public static final ConfigProperty<String> PRECOMBINE_FIELD_TYPE_PROP = ConfigProperty
        .key("hoodie.deltastreamer.source.json.kafka.post.processor.maxwell.precombine.field.type")
        .defaultValue(DATE_STRING.toString())
        .withDocumentation("Data type of the preCombine field. could be NON_TIMESTAMP, DATE_STRING,"
            + "UNIX_TIMESTAMP or EPOCHMILLISECONDS. DATE_STRING by default ");

    public static final ConfigProperty<String> PRECOMBINE_FIELD_FORMAT_PROP = ConfigProperty
        .key("hoodie.deltastreamer.source.json.kafka.post.processor.maxwell.precombine.field.format")
        .defaultValue("yyyy-MM-dd HH:mm:ss")
        .withDocumentation("When the preCombine filed is in DATE_STRING format, use should tell hoodie"
            + "what format it is. 'yyyy-MM-dd HH:mm:ss' by default");
  }

  @Override
  public JavaRDD<String> process(JavaRDD<String> maxwellJsonRecords) {
    return maxwellJsonRecords.map(record -> {
      JsonNode inputJson = MAPPER.readTree(record);
      String database = inputJson.get(DATABASE).textValue();
      String table = inputJson.get(TABLE).textValue();

      // filter out target databases and tables
      if (isTargetTable(database, table)) {
        ObjectNode result = (ObjectNode) inputJson.get(DATA);
        String type = inputJson.get(OPERATION_TYPE).textValue();

        // insert or update
        if (INSERT.equals(type) || UPDATE.equals(type)) {
          // tag this record not delete.
          result.put(HoodieRecord.HOODIE_IS_DELETED, false);
          return result.toString();

          // delete
        } else if (DELETE.equals(type)) {
          return processDelete(inputJson, result);
        } else {
          // there might be some ddl data, ignore it
          return null;
        }
      } else {
        // not the data from target table(s), ignore it
        return null;
      }
    }).filter(Objects::nonNull);
  }

  private String processDelete(JsonNode inputJson, ObjectNode result) {
    // tag this record as delete.
    result.put(HoodieRecord.HOODIE_IS_DELETED, true);

    PreCombineFieldType preCombineFieldType =
        valueOf(this.props.getString(Config.PRECOMBINE_FIELD_TYPE_PROP.key(),
            Config.PRECOMBINE_FIELD_TYPE_PROP.defaultValue()).toUpperCase(Locale.ROOT));

    // maxwell won't update the `update_time`(delete time) field of the record which is tagged as delete. so if we
    // want to delete this record correctly, we should update its `update_time` to a time closer to where the
    // delete operation actually occurred. here we use `ts` from maxwell json string as this 'delete' time.

    // we can update the `update_time`(delete time) only when it is in timestamp format.
    if (!preCombineFieldType.equals(NON_TIMESTAMP)) {
      String preCombineField = this.props.getString(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key());

      // ts from maxwell
      long ts = inputJson.get(TS).longValue();

      // convert the `update_time`(delete time) to the proper format.
      if (preCombineFieldType.equals(DATE_STRING)) {
        // DATE_STRING format
        String timeFormat = this.props.getString(Config.PRECOMBINE_FIELD_FORMAT_PROP.key(), Config.PRECOMBINE_FIELD_FORMAT_PROP.defaultValue());
        result.put(preCombineField, DateTimeUtils.formatUnixTimestamp(ts, timeFormat));
      } else if (preCombineFieldType.equals(EPOCHMILLISECONDS)) {
        // EPOCHMILLISECONDS format
        result.put(preCombineField, ts * 1000L);
      } else if (preCombineFieldType.equals(UNIX_TIMESTAMP)) {
        // UNIX_TIMESTAMP format
        result.put(preCombineField, ts);
      } else {
        throw new HoodieSourcePostProcessException("Unsupported preCombine time format " + preCombineFieldType);
      }
    }
    return result.toString();
  }

  /**
   * Check if it is the right table we want to consume from.
   *
   * @param database database the data belong to
   * @param table    table the data belong to
   */
  private boolean isTargetTable(String database, String table) {
    if (!databaseRegex.isPresent()) {
      return Pattern.matches(tableRegex, table);
    } else {
      return Pattern.matches(databaseRegex.get(), database) && Pattern.matches(tableRegex, table);
    }
  }

}
