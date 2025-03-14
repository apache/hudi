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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.DateTimeUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.config.JsonKafkaPostProcessorConfig;
import org.apache.hudi.utilities.exception.HoodieSourcePostProcessException;
import org.apache.hudi.utilities.sources.processor.JsonKafkaSourcePostProcessor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.api.java.JavaRDD;

import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
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
    databaseRegex = Option.ofNullable(getStringWithAltKeys(props, JsonKafkaPostProcessorConfig.DATABASE_NAME_REGEX, true));
    tableRegex = getStringWithAltKeys(props, JsonKafkaPostProcessorConfig.TABLE_NAME_REGEX);
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
          result.put(HoodieRecord.HOODIE_IS_DELETED_FIELD, false);
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
    result.put(HoodieRecord.HOODIE_IS_DELETED_FIELD, true);

    PreCombineFieldType preCombineFieldType =
        valueOf(getStringWithAltKeys(
            this.props, JsonKafkaPostProcessorConfig.PRECOMBINE_FIELD_TYPE, true).toUpperCase(Locale.ROOT));

    // maxwell won't update the `update_time`(delete time) field of the record which is tagged as delete. so if we
    // want to delete this record correctly, we should update its `update_time` to a time closer to where the
    // delete operation actually occurred. here we use `ts` from maxwell json string as this 'delete' time.

    // we can update the `update_time`(delete time) only when it is in timestamp format.
    if (!preCombineFieldType.equals(NON_TIMESTAMP)) {
      String preCombineField = this.props.getString(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), null);

      // ts from maxwell
      long ts = inputJson.get(TS).longValue();

      // convert the `update_time`(delete time) to the proper format.
      if (preCombineFieldType.equals(DATE_STRING)) {
        // DATE_STRING format
        String timeFormat = getStringWithAltKeys(this.props, JsonKafkaPostProcessorConfig.PRECOMBINE_FIELD_FORMAT, true);
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
