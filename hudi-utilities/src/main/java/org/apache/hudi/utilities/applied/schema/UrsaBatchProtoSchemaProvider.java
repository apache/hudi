/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.applied.schema;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.applied.common.AppliedUtils;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;

public class UrsaBatchProtoSchemaProvider {
  private static final Logger LOG = LoggerFactory.getLogger(UrsaBatchProtoSchemaProvider.class);
  private static final String PROTO_LOCAL_SCHEMA_PREFIX = "ursa_file_descriptor_set_";
  private static final String PROTO_LOCAL_SCHEMA_SUFFIX = ".pb";

  private final String schemaFileBucketRegion;
  private final String schemaFileBucketName;
  private final String schemaFilePrefix;
  private final String deserializedParentColName;
  private final String protoMessageTypeName;
  private final String serializedSourceColumn;
  private String latestSchemaS3Url;
  private byte[] fileDescriptorSchema;
  private Schema sourceSchema;
  private File protoLatestLocalSchemaFile;

  public UrsaBatchProtoSchemaProvider(TypedProperties props) {
    checkRequiredConfigProperties(props,
        Arrays.asList(UrsaBatchProtoBasedSchemaProviderConfig.PROTO_SCHEMA_BUCKET,
            UrsaBatchProtoBasedSchemaProviderConfig.PROTO_SCHEMA_BUCKET_REGION,
            UrsaBatchProtoBasedSchemaProviderConfig.PROTO_SCHEMA_PREFIX));
    this.schemaFilePrefix = props.getString(UrsaBatchProtoBasedSchemaProviderConfig.PROTO_SCHEMA_PREFIX.key());
    this.schemaFileBucketName = props.getString(UrsaBatchProtoBasedSchemaProviderConfig.PROTO_SCHEMA_BUCKET.key());
    this.schemaFileBucketRegion = props.getString(UrsaBatchProtoBasedSchemaProviderConfig.PROTO_SCHEMA_BUCKET_REGION.key());
    this.deserializedParentColName = props.getString(UrsaBatchProtoBasedSchemaProviderConfig.PROTO_DESERIALIZED_PARENT_COL_NAME.key());
    this.protoMessageTypeName = props.getString(UrsaBatchProtoBasedSchemaProviderConfig.PROTO_SCHEMA_MESSAGE_TYPE.key());
    this.serializedSourceColumn = props.getString(UrsaBatchProtoBasedSchemaProviderConfig.PROTO_SERIALIZED_INPUT_COL.key());
    refresh();
  }

  public void refresh() {
    String newLatestSchemaFile = getLatestSchemaFile(schemaFileBucketRegion, schemaFileBucketName, schemaFilePrefix);
    if ((newLatestSchemaFile != null) && !(newLatestSchemaFile.equals(latestSchemaS3Url))) {
      LOG.info("Discovered new schema file version in " + newLatestSchemaFile);
      this.fileDescriptorSchema = AppliedUtils.getS3ObjectAsBytes(schemaFileBucketRegion, schemaFileBucketName, newLatestSchemaFile);
      this.latestSchemaS3Url = newLatestSchemaFile;
      try {
        File tempFile = File.createTempFile(PROTO_LOCAL_SCHEMA_PREFIX, PROTO_LOCAL_SCHEMA_SUFFIX);
        tempFile.deleteOnExit();
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
          os.write(this.fileDescriptorSchema);
        }
        this.protoLatestLocalSchemaFile = tempFile;
      } catch (IOException ioe) {
        LOG.error("Unable to save latest proto schema to local file", ioe);
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    }

    if (this.fileDescriptorSchema == null) {
      throw new HoodieException("No Protobuf schema found!!");
    }
  }

  private static String getLatestSchemaFile(String region, String bucket, String prefix) {
    try {
      LOG.info("Fetching latest protobuf schema from S3 !!");
      S3Client s3Client = S3Client.builder().region(Region.of(region)).build();
      ListObjectsRequest objectsRequest = ListObjectsRequest.builder().bucket(bucket).prefix(prefix).build();
      String latestKey = null;
      int maxVersionSeen = -1;
      ListObjectsResponse response = s3Client.listObjects(objectsRequest);
      if (response.hasContents()) {
        for (S3Object s3Object : response.contents()) {
          String key = s3Object.key();
          if (key.contains(".")) {
            String[] parts = key.split("\\.");
            if ((parts.length > 1) && (parts[parts.length - 1].startsWith("v"))) {
              try {
                int version = Integer.parseInt(parts[parts.length - 1].substring(1));
                LOG.info("Found version " + version + "in schema file " + key);
                if (maxVersionSeen < version) {
                  maxVersionSeen = version;
                  latestKey = key;
                  LOG.info("Found Later version " + version + "in schema file " + key);
                }
              } catch (NumberFormatException nfe) {
                LOG.warn("Unable to extract version in schema file " + key, nfe);
              }
            }
          }
        }
      }
      return latestKey;
    } catch (S3Exception e) {
      throw new HoodieException("Error reading schema file from S3", e);
    } catch (Exception e) {
      throw new HoodieException("Error reading schema file", e);
    }
  }

  public String getDeserializedParentColName() {
    return deserializedParentColName;
  }

  public String getProtoMessageTypeName() {
    return protoMessageTypeName;
  }

  public byte[] getFileDescriptorSchema() {
    return fileDescriptorSchema;
  }

  public String getSerializedSourceColumn() {
    return serializedSourceColumn;
  }

  public File getProtoLatestLocalSchemaFile() {
    return protoLatestLocalSchemaFile;
  }
}
