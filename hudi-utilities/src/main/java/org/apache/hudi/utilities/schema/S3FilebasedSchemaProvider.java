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
//import java.net.URISyntaxException;
//import java.util.Collections;
//import java.net.URI;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.avro.Schema;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.spark.api.java.JavaSparkContext;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * A simple schema provider, that reads off files on S3.
 */
public class S3FilebasedSchemaProvider extends SchemaProvider {

  /**
   * Configs supported.
   */
  public static class Config {
    //private static final String SOURCE_SCHEMA_FILE_PROP = "hoodie.deltastreamer.schemaprovider" + ".source.schema.file";
    //private static final String SOURCE_SCHEMA_TYPE_PROP = "hoodie.deltastreamer.schemaprovider" + ".source.schema.type";
    private static final String SOURCE_SCHEMA_S3_BUCKET = "hoodie.deltastreamer.schemaprovider" + ".source.schema.bucket";
    private static final String SOURCE_SCHEMA_REGION    = "hoodie.deltastreamer.schemaprovider" + ".source.schema.region";
    private static final String SOURCE_SCHEMA_FILENAME  = "hoodie.deltastreamer.schemaprovider" + ".source.schema.name";
  }

  private static final Logger LOG = LogManager.getLogger(S3FilebasedSchemaProvider.class);
  //private final FileSystem fs;

  private final Schema sourceSchema;

  public S3FilebasedSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    //DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(Config.SOURCE_SCHEMA_FILE_PROP));
    //DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(Config.SOURCE_SCHEMA_TYPE_PROP));
    try {
      LOG.info("S3 Path of the schema file: " + props.getString(Config.SOURCE_SCHEMA_S3_BUCKET) + "/" + props.getString(Config.SOURCE_SCHEMA_FILENAME));
      AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(props.getString(Config.SOURCE_SCHEMA_REGION)).build();
      S3Object object = s3Client.getObject(new GetObjectRequest(props.getString(Config.SOURCE_SCHEMA_S3_BUCKET), props.getString(Config.SOURCE_SCHEMA_FILENAME)));

      InputStream stream = object.getObjectContent();
      this.sourceSchema = new Schema.Parser().parse(stream);
      //URI schemaPathUri = new URI(props.getString(Config.SOURCE_SCHEMA_FILE_PROP) + "/" + props.getString(Config.SOURCE_SCHEMA_TYPE_PROP) + ".avsc");
      //this.fs = FileSystem.get(schemaPathUri, jssc.hadoopConfiguration());
      //this.sourceSchema = new Schema.Parser().parse(this.fs.open(new Path(schemaPathUri)));
    } catch (IOException ioe) {
      throw new HoodieIOException("Error reading schema From S3", ioe);
    }
    //catch (URISyntaxException urie) {
    //throw new HoodieIOException("Error getting S3 URI", new IOException());
    //}
  }

  @Override
  public Schema getSourceSchema() {
    return sourceSchema;
  }
}
