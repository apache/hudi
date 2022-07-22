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

package org.apache.hudi.avro;

import org.apache.avro.generic.GenericData;

import java.io.Serializable;

/**
 * In some cases like putting the [[GenericData.Record]] into [[ExternalSpillableMap]],
 * objects is asked to extend [[Serializable]].
 *
 * This class wraps [[GenericData.Record]].
 */
public class SerializableRecord implements Serializable {

  private GenericData.Record record;

  public SerializableRecord(GenericData.Record record) {
    this.record = record;
  }

  public GenericData.Record getRecord() {
    return this.record;
  }
}
