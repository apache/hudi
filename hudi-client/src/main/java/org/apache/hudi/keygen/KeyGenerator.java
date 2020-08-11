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

package org.apache.hudi.keygen;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.List;

/**
 * Abstract class to extend for plugging in extraction of {@link HoodieKey} from an Avro record.
 */
public abstract class KeyGenerator implements Serializable {

  protected transient TypedProperties config;

  protected KeyGenerator(TypedProperties config) {
    this.config = config;
  }

  /**
   * Generate a Hoodie Key out of provided generic record.
   */
  public abstract HoodieKey getKey(GenericRecord record);

  /**
   * Used during bootstrap, to project out only the record key fields from bootstrap source dataset.
   *
   * @return list of field names, when concatenated make up the record key.
   */
  public List<String> getRecordKeyFieldNames() {
    throw new UnsupportedOperationException("Bootstrap not supported for key generator. "
        + "Please override this method in your custom key generator.");
  }

  /**
   * Initializes {@link KeyGenerator} for {@link Row} based operations.
   * @param structType structype of the dataset.
   * @param structName struct name of the dataset.
   * @param recordNamespace record namespace of the dataset.
   */
  public void initializeRowKeyGenerator(StructType structType, String structName, String recordNamespace) {
    throw new UnsupportedOperationException("Expected to be overridden by sub classes, to improve performance for spark datasource writes ");
  }

  /**
   * Fetch record key from {@link Row}.
   * @param row instance of {@link Row} from which record key is requested.
   * @return the record key of interest from {@link Row}.
   */
  public String getRecordKey(Row row) {
    throw new UnsupportedOperationException("Expected to be overridden by sub classes, to improve performance for spark datasource writes ");
  }

  /**
   * Fetch partition path from {@link Row}.
   * @param row instance of {@link Row} from which partition path is requested
   * @return the partition path of interest from {@link Row}.
   */
  public String getPartitionPath(Row row) {
    throw new UnsupportedOperationException("Expected to be overridden by sub classes, to improve performance for spark datasource writes ");
  }
}
