/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.utilities.keygen;

import com.uber.hoodie.DataSourceUtils;
import com.uber.hoodie.SimpleKeyGenerator;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.exception.HoodieNotSupportedException;
import com.uber.hoodie.utilities.exception.HoodieDeltaStreamerException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;
import org.apache.avro.generic.GenericRecord;

/**
 * Key generator, that relies on timestamps for partitioning field. Still picks record key by name.
 */
public class TimestampBasedKeyGenerator extends SimpleKeyGenerator {

  enum TimestampType implements Serializable {
    UNIX_TIMESTAMP, DATE_STRING, MIXED
  }

  private final TimestampType timestampType;

  private SimpleDateFormat inputDateFormat;

  private final String outputDateFormat;


  /**
   * Supported configs
   */
  static class Config {

    // One value from TimestampType above
    private static final String TIMESTAMP_TYPE_FIELD_PROP = "hoodie.deltastreamer.keygen"
        + ".timebased.timestamp.type";
    private static final String TIMESTAMP_INPUT_DATE_FORMAT_PROP = "hoodie.deltastreamer.keygen"
        + ".timebased.input"
        + ".dateformat";
    private static final String TIMESTAMP_OUTPUT_DATE_FORMAT_PROP = "hoodie.deltastreamer.keygen"
        + ".timebased.output"
        + ".dateformat";
  }

  public TimestampBasedKeyGenerator(TypedProperties config) {
    super(config);
    DataSourceUtils.checkRequiredProperties(config,
        Arrays.asList(Config.TIMESTAMP_TYPE_FIELD_PROP, Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP));
    this.timestampType = TimestampType.valueOf(config.getString(Config.TIMESTAMP_TYPE_FIELD_PROP));
    this.outputDateFormat = config.getString(Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP);

    if (timestampType == TimestampType.DATE_STRING || timestampType == TimestampType.MIXED) {
      DataSourceUtils
          .checkRequiredProperties(config, Arrays.asList(Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP));
      this.inputDateFormat = new SimpleDateFormat(
          config.getString(Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP));
      this.inputDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    }
  }

  @Override
  public HoodieKey getKey(GenericRecord record) {
    Object partitionVal = record.get(partitionPathField);
    SimpleDateFormat partitionPathFormat = new SimpleDateFormat(outputDateFormat);
    partitionPathFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

    try {
      long unixTime;
      if (partitionVal instanceof Double) {
        unixTime = ((Double) partitionVal).longValue();
      } else if (partitionVal instanceof Float) {
        unixTime = ((Float) partitionVal).longValue();
      } else if (partitionVal instanceof Long) {
        unixTime = (Long) partitionVal;
      } else if (partitionVal instanceof String) {
        unixTime = inputDateFormat.parse(partitionVal.toString()).getTime() / 1000;
      } else {
        throw new HoodieNotSupportedException(
            "Unexpected type for partition field: " + partitionVal.getClass().getName());
      }

      return new HoodieKey(record.get(recordKeyField).toString(),
          partitionPathFormat.format(new Date(unixTime * 1000)));
    } catch (ParseException pe) {
      throw new HoodieDeltaStreamerException(
          "Unable to parse input partition field :" + partitionVal, pe);
    }
  }
}
