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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * This is a mongo  schema provider, that reads off mongo schema files on S3.
 */
public class MongoSchemaProvider extends SchemaProvider {

  /**
   * Configs supported.
   */
  public static class Config {
    private static final String SOURCE_SCHEMA_S3_BUCKET = "hoodie.deltastreamer.schemaprovider" + ".source.schema.bucket";
    private static final String SOURCE_SCHEMA_REGION    = "hoodie.deltastreamer.schemaprovider" + ".source.schema.region";
    private static final String SOURCE_SCHEMA_FILENAME  = "hoodie.deltastreamer.schemaprovider" + ".source.schema.name";
    private static final String SOURCE_SCHEMA_EXPIRED   = "hoodie.deltastreamer.schemaprovider" + ".source.schema.expired";
  }

  /*
   * Embedded schema provider that build oplog schemas
   */
  private static class OplogSchemaProvider {
    private Schema baseSchema;
    private final String[] fieldNames = new String[]{
        "_id", "_op", "_ts_ms", "_patch"
    };

    private final Type[] fieldTypes = new Type[]{
        Type.STRING, Type.STRING, Type.LONG, Type.STRING
    };

    private Schema buildOplogBaseSchema() {
      SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder
          .record("MongoOplog")
          .namespace("com.wish.log")
          .fields();

      Schema nullSchema = Schema.create(Schema.Type.NULL);

      for (int i = 0; i < fieldNames.length; ++i) {
        Schema schema = Schema.create(fieldTypes[i]);
        Schema unionSchema = Schema.createUnion(Arrays.asList(nullSchema, schema));

        fieldAssembler.name(fieldNames[i]).type(unionSchema).withDefault(null);
      }

      return fieldAssembler.endRecord();
    }

    private OplogSchemaProvider() {
      baseSchema = buildOplogBaseSchema();
    }

    public Schema getBaseSchema() {
      return this.baseSchema;
    }
  }

  private static final Logger LOG = LogManager.getLogger(MongoSchemaProvider.class);

  private final String s3Bucket;
  private final String s3File;
  private final long schemaExpiredTime;

  private Schema sourceSchema;
  private static final OplogSchemaProvider OPLOG_SCHEMA_PROVIDER = new OplogSchemaProvider();
  private AmazonS3 s3Client;
  private long lastModifiedTS;
  private long schemaCachedTS;

  private static long getTimestamp(Date date) {
    return date.getTime() / 1000L;
  }

  public MongoSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    this.s3Bucket = props.getString(Config.SOURCE_SCHEMA_S3_BUCKET);
    this.s3File = props.getString(Config.SOURCE_SCHEMA_FILENAME);

    LOG.info("S3 Path of the schema file: " + this.s3Bucket + "/" + this.s3File);

    this.schemaExpiredTime = Long.parseLong(props.getString(Config.SOURCE_SCHEMA_EXPIRED));

    try {
      s3Client = AmazonS3ClientBuilder.standard()
          .withRegion(props.getString(Config.SOURCE_SCHEMA_REGION)).build();
      fetchSchemaFromS3();
    } catch (IOException ioe) {
      throw new HoodieIOException("Error reading schema From S3", ioe);
    }
  }

  @Override
  public Schema getSourceSchema() {
    return sourceSchema;
  }

  @Override
  public Pair<Schema, Boolean> getLatestSourceSchema() {
    long currentTimestamp = getTimestamp(new Date());
    boolean schemaChanged = false;
    if (currentTimestamp - schemaCachedTS >= schemaExpiredTime) {
      LOG.info("Fetching the latest schema......");
      try {
        schemaChanged = fetchSchemaFromS3();
      } catch (IOException ioe) {
        LOG.error("Got errors while fetching the new schema", ioe);
      }
    }
    return Pair.of(sourceSchema, schemaChanged);
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
   * Return true if latest schema is fetched, and false if the schema has not changed.
   */
  private boolean fetchSchemaFromS3() throws IOException {
    S3Object s3Object = s3Client.getObject(getSchemaRequest());
    InputStream stream = s3Object.getObjectContent();
    boolean schemaChanged = false;
    try {
      long fileTS = getTimestamp(s3Object.getObjectMetadata().getLastModified());
      if (sourceSchema == null || fileTS == 0 || lastModifiedTS != fileTS) {
        Schema schemaCache = new Schema.Parser().parse(stream);
        if (schemaCache.getType() != Type.RECORD) {
          throw new IllegalArgumentException("Record schema type is expected");
        }
        sourceSchema = combineSchemaFromS3(schemaCache);
        lastModifiedTS = fileTS;
        schemaChanged = true;
      }
      schemaCachedTS = getTimestamp(new Date());
      return schemaChanged;
    } finally {
      readStream(stream);
      stream.close();
    }
  }

  /**
   * Combine Mongo table and oplog schemas.
   */
  private Schema combineSchemaFromS3(Schema tableSchema) throws IOException {
    Schema oplogSchema = OPLOG_SCHEMA_PROVIDER.getBaseSchema();

    List<Schema.Field> fieldList = oplogSchema.getFields().stream().map(
        field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()))
        .collect(Collectors.toList());

    for (Schema.Field f : tableSchema.getFields()) {
      Schema.Field ff = new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal());
      for (String alias : f.aliases()) {
        ff.addAlias(alias);
      }
      fieldList.add(ff);
    }

    return Schema.createRecord(
        tableSchema.getName(), tableSchema.getDoc(),
        tableSchema.getNamespace(), tableSchema.isError(), fieldList);
  }

  /**
   * Construct S3 object request for Avro schema file.
   */
  private GetObjectRequest getSchemaRequest() {
    return new GetObjectRequest(this.s3Bucket, this.s3File);
  }
}
