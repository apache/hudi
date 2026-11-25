/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.Getter;

import java.io.Serializable;

/**
 * The record that a record replaces when a write moves the new version of a record to another file group, see
 * {@link HoodieRecord#getReplacedRecord()}: the location of the replaced record in the base file of its file group,
 * and once resolved, the values of the format meta columns for it, see
 * {@link org.apache.hudi.common.FormatMetaColumn#valueOnUpdate}. It is carried within one write only.
 */
@Getter
public final class ReplacedRecord implements KryoSerializable, Serializable {

  private static final long serialVersionUID = 1L;

  private HoodieRecordGlobalLocation location;
  // The values the format meta columns resolve to for the replaced record, null until resolved
  private Object[] formatMetaColumnValues;

  public ReplacedRecord(HoodieRecordGlobalLocation location) {
    this(location, null);
  }

  private ReplacedRecord(HoodieRecordGlobalLocation location, Object[] formatMetaColumnValues) {
    this.location = location;
    this.formatMetaColumnValues = formatMetaColumnValues;
  }

  // For Kryo
  private ReplacedRecord() {
  }

  /**
   * Returns the replaced record with the given values of the format meta columns resolved for it.
   */
  public ReplacedRecord withFormatMetaColumnValues(Object[] formatMetaColumnValues) {
    return new ReplacedRecord(location, formatMetaColumnValues);
  }

  @Override
  public void write(Kryo kryo, Output output) {
    kryo.writeObject(output, location);
    // The number of values plus one, 0 while unresolved
    output.writeInt(formatMetaColumnValues == null ? 0 : formatMetaColumnValues.length + 1, true);
    if (formatMetaColumnValues != null) {
      for (Object value : formatMetaColumnValues) {
        kryo.writeClassAndObject(output, value);
      }
    }
  }

  @Override
  public void read(Kryo kryo, Input input) {
    this.location = kryo.readObject(input, HoodieRecordGlobalLocation.class);
    int length = input.readInt(true) - 1;
    if (length >= 0) {
      this.formatMetaColumnValues = new Object[length];
      for (int i = 0; i < length; i++) {
        formatMetaColumnValues[i] = kryo.readClassAndObject(input);
      }
    }
  }

  @Override
  public String toString() {
    return "ReplacedRecord{location=" + location + ", resolved=" + (formatMetaColumnValues != null) + '}';
  }
}
