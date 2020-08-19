/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.keygen.parser;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;

public interface HoodieDateTimeParser extends Serializable {

  /**
   * Returns the output date format in which the partition paths will be created for the hudi dataset.
   * @return
   */
  String getOutputDateFormat();

  /**
   * Returns input formats in which datetime based values might be coming in incoming records.
   * @return
   */
  DateTimeFormatter getInputFormatter();

  /**
   * Returns the datetime zone one should expect the incoming values into.
   * @return
   */
  DateTimeZone getInputDateTimeZone();

  /**
   * Returns the datetime zone using which the final partition paths for hudi dataset are created.
   * @return
   */
  DateTimeZone getOutputDateTimeZone();
}
