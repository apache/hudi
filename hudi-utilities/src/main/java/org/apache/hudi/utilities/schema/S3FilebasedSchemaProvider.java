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

package org.apache.hudi.utilities.schema;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.spark.api.java.JavaSparkContext;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;

/**
 * A simple schema provider, that reads off files on S3.
 */
public class S3FilebasedSchemaProvider extends SchemaProvider {

  /**
   * Configs supported.
   */
  public static class Config {
    private static final String SOURCE_SCHEMA_S3_BUCKET = "hoodie.deltastreamer.schemaprovider" + ".source.schema.bucket";
    private static final String SOURCE_SCHEMA_REGION    = "hoodie.deltastreamer.schemaprovider" + ".source.schema.region";
    private static final String SOURCE_SCHEMA_FILENAME  = "hoodie.deltastreamer.schemaprovider" + ".source.schema.name";
    private static final String SOURCE_SCHEMA_EXPIRED   = "hoodie.deltastreamer.schemaprovider" + ".source.schema.expired";
  }

  private static final Logger LOG = LogManager.getLogger(S3FilebasedSchemaProvider.class);

  private final String s3Bucket;
  private final String s3File;
  private final long schemaExpiredTime;

  private Schema sourceSchema;
  private AmazonS3 s3Client;
  private long schemaCachedTS;

  public S3FilebasedSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    this.s3Bucket = props.getString(Config.SOURCE_SCHEMA_S3_BUCKET);
    this.s3File   = props.getString(Config.SOURCE_SCHEMA_FILENAME);

    LOG.info("S3 Path of the schema file: " + this.s3Bucket + "/" + this.s3File);

    this.schemaCachedTS = (new Date()).getTime() / 1000L;
    this.schemaExpiredTime = Long.parseLong(props.getString(Config.SOURCE_SCHEMA_EXPIRED));
    
    try {
      s3Client = AmazonS3ClientBuilder.standard().withRegion(props.getString(Config.SOURCE_SCHEMA_REGION)).build();
      this.sourceSchema = getSchemaFromS3();
    } catch (IOException ioe) {
      throw new HoodieIOException("Error reading schema From S3", ioe);
    }
  }

  @Override
  public Schema getSourceSchema() {
    long currentTimestamp = (new Date()).getTime() / 1000L;
    if (currentTimestamp - schemaCachedTS >= schemaExpiredTime) {
      LOG.info("Fetching new schema......");
      try {
        this.sourceSchema = getSchemaFromS3();
        this.schemaCachedTS = currentTimestamp;
      } catch (IOException ioe) {
        LOG.error("Got errors while fetching the new schema", ioe);
      }
    }
    
    return sourceSchema;
  }

  /**
   * Read all bytes from stream.
   */
  private void readStream(InputStream stream) throws IOException {
    byte[] messageByte = new byte[512];
    int bytesRead = messageByte.length;
    while (bytesRead > 0) {
      bytesRead = stream.read(messageByte);
    }
  }

  /**
   * Read Avro Schema from S3.
   */
  private Schema getSchemaFromS3() throws IOException {
    InputStream stream = s3Client.getObject(getSchemaRequest()).getObjectContent();
    try {
      Schema schemaCache = new Schema.Parser().parse(stream);
      if (schemaCache.getType() != Type.RECORD) {
        throw new IllegalArgumentException("Record schema type is expected");
      }

      return schemaCache;
    } finally {
      readStream(stream);
      stream.close();
    }
  }

  /**
   * Construct S3 object request for Avro schema file.
   */
  private GetObjectRequest getSchemaRequest() {
    return new GetObjectRequest(this.s3Bucket, this.s3File);
  }
}
