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

package org.apache.hudi.common.config;

import org.apache.hudi.common.model.HoodieFunctionalIndexDefinition;
import org.apache.hudi.common.util.BinaryUtil;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.secondary.SecondaryIndexType;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.apache.hudi.common.util.ConfigUtils.recoverIfNeeded;
import static org.apache.hudi.common.util.HoodieConfigUtils.fetchConfigs;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

@Immutable
@ConfigClassProperty(name = "Common Index Configs",
    groupName = ConfigGroups.Names.INDEXING,
    subGroupName = ConfigGroups.SubGroupNames.FUNCTIONAL_INDEX,
    areCommonConfigs = true,
    description = "")
public class HoodieFunctionalIndexConfig extends HoodieConfig {

  public static final String INDEX_DEFINITION_FILE = "index.properties";
  public static final String INDEX_DEFINITION_FILE_BACKUP = "index.properties.backup";
  public static final ConfigProperty<String> INDEX_NAME = ConfigProperty
      .key("hoodie.functional.index.name")
      .noDefaultValue()
      .sinceVersion("1.0.0")
      .withDocumentation("Name of the functional index. This is also used for the partition name in the metadata table.");

  public static final ConfigProperty<String> INDEX_TYPE = ConfigProperty
      .key("hoodie.functional.index.type")
      .defaultValue(MetadataPartitionType.COLUMN_STATS.name())
      .withValidValues(
          MetadataPartitionType.COLUMN_STATS.name(),
          MetadataPartitionType.BLOOM_FILTERS.name()
      )
      .sinceVersion("1.0.0")
      .withDocumentation("Type of the functional index. Default is `column_stats` if there are no functions and expressions in the command. "
          + "Valid options could be BITMAP, COLUMN_STATS, LUCENE, etc. If index_type is not provided, "
          + "and there are functions or expressions in the command then a functional index using column stats will be created.");

  public static final ConfigProperty<String> INDEX_FUNCTION = ConfigProperty
      .key("hoodie.functional.index.function")
      .noDefaultValue()
      .sinceVersion("1.0.0")
      .withDocumentation("Function to be used for building the functional index.");

  public static final ConfigProperty<String> INDEX_DEFINITION_CHECKSUM = ConfigProperty
      .key("hoodie.table.checksum")
      .noDefaultValue()
      .sinceVersion("1.0.0")
      .withDocumentation("Index definition checksum is used to guard against partial writes in HDFS. "
          + "It is added as the last entry in index.properties and then used to validate while reading table config.");

  private static final String INDEX_DEFINITION_CHECKSUM_FORMAT = "%s.%s"; // <index_name>.<index_type>

  public HoodieFunctionalIndexConfig() {
    super();
  }

  public static void create(HoodieStorage storage, StoragePath metadataFolder, Properties properties)
      throws IOException {
    if (!storage.exists(metadataFolder)) {
      storage.createDirectory(metadataFolder);
    }
    HoodieConfig hoodieConfig = new HoodieConfig(properties);
    StoragePath propertyPath = new StoragePath(metadataFolder, INDEX_DEFINITION_FILE);
    try (OutputStream outputStream = storage.create(propertyPath)) {
      if (!hoodieConfig.contains(INDEX_NAME)) {
        throw new IllegalArgumentException(INDEX_NAME.key() + " property needs to be specified");
      }
      hoodieConfig.setDefaultValue(INDEX_TYPE);
      storeProperties(hoodieConfig.getProps(), outputStream);
    }
  }

  public static void update(HoodieStorage storage, StoragePath metadataFolder,
                            Properties updatedProps) {
    modify(storage, metadataFolder, updatedProps, ConfigUtils::upsertProperties);
  }

  public static void delete(HoodieStorage storage, StoragePath metadataFolder,
                            Set<String> deletedProps) {
    Properties props = new Properties();
    deletedProps.forEach(p -> props.setProperty(p, ""));
    modify(storage, metadataFolder, props, ConfigUtils::deleteProperties);
  }

  public static void recover(HoodieStorage storage, StoragePath metadataFolder)
      throws IOException {
    StoragePath cfgPath = new StoragePath(metadataFolder, INDEX_DEFINITION_FILE);
    StoragePath backupCfgPath = new StoragePath(metadataFolder, INDEX_DEFINITION_FILE_BACKUP);
    recoverIfNeeded(storage, cfgPath, backupCfgPath);
  }

  private static void modify(HoodieStorage storage, StoragePath metadataFolder,
                             Properties modifyProps, BiConsumer<Properties, Properties> modifyFn) {
    StoragePath cfgPath = new StoragePath(metadataFolder, INDEX_DEFINITION_FILE);
    StoragePath backupCfgPath = new StoragePath(metadataFolder, INDEX_DEFINITION_FILE_BACKUP);
    try {
      // 0. do any recovery from prior attempts.
      recoverIfNeeded(storage, cfgPath, backupCfgPath);

      // 1. Read the existing config
      TypedProperties props =
          fetchConfigs(storage, metadataFolder.toString(), INDEX_DEFINITION_FILE,
              INDEX_DEFINITION_FILE_BACKUP, MAX_READ_RETRIES, READ_RETRY_DELAY_MSEC);

      // 2. backup the existing properties.
      try (OutputStream out = storage.create(backupCfgPath, false)) {
        storeProperties(props, out);
      }

      // 3. delete the properties file, reads will go to the backup, until we are done.
      storage.deleteFile(cfgPath);

      // 4. Upsert and save back.
      String checksum;
      try (OutputStream out = storage.create(cfgPath, true)) {
        modifyFn.accept(props, modifyProps);
        checksum = storeProperties(props, out);
      }

      // 4. verify and remove backup.
      try (InputStream in = storage.open(cfgPath)) {
        props.clear();
        props.load(in);
        if (!props.containsKey(INDEX_DEFINITION_CHECKSUM.key())
            || !props.getProperty(INDEX_DEFINITION_CHECKSUM.key()).equals(checksum)) {
          // delete the properties file and throw exception indicating update failure
          // subsequent writes will recover and update, reads will go to the backup until then
          storage.deleteFile(cfgPath);
          throw new HoodieIOException("Checksum property missing or does not match.");
        }
      }

      // 5. delete the backup properties file
      storage.deleteFile(backupCfgPath);
    } catch (IOException e) {
      throw new HoodieIOException("Error updating table configs.", e);
    }
  }

  private static Properties getOrderedPropertiesWithTableChecksum(Properties props) {
    Properties orderedProps = new OrderedProperties(props);
    orderedProps.put(INDEX_DEFINITION_CHECKSUM.key(), String.valueOf(generateChecksum(props)));
    return orderedProps;
  }

  /**
   * Write the properties to the given output stream and return the table checksum.
   *
   * @param props        - properties to be written
   * @param outputStream - output stream to which properties will be written
   * @return return the table checksum
   */
  private static String storeProperties(Properties props, OutputStream outputStream)
      throws IOException {
    final String checksum;
    if (isValidChecksum(props)) {
      checksum = props.getProperty(INDEX_DEFINITION_CHECKSUM.key());
      props.store(outputStream, "Updated at " + Instant.now());
    } else {
      Properties propsWithChecksum = getOrderedPropertiesWithTableChecksum(props);
      propsWithChecksum.store(outputStream, "Properties saved on " + Instant.now());
      checksum = propsWithChecksum.getProperty(INDEX_DEFINITION_CHECKSUM.key());
      props.setProperty(INDEX_DEFINITION_CHECKSUM.key(), checksum);
    }
    return checksum;
  }

  private static boolean isValidChecksum(Properties props) {
    return props.containsKey(INDEX_DEFINITION_CHECKSUM.key()) && validateChecksum(props);
  }

  public static long generateChecksum(Properties props) {
    if (!props.containsKey(INDEX_NAME.key())) {
      throw new IllegalArgumentException(INDEX_NAME.key() + " property needs to be specified");
    }
    String table = props.getProperty(INDEX_NAME.key());
    String database = props.getProperty(INDEX_TYPE.key(), "");
    return BinaryUtil.generateChecksum(getUTF8Bytes(String.format(INDEX_DEFINITION_CHECKSUM_FORMAT, database, table)));
  }

  public static boolean validateChecksum(Properties props) {
    return Long.parseLong(props.getProperty(INDEX_DEFINITION_CHECKSUM.key())) == generateChecksum(props);
  }

  public static HoodieFunctionalIndexConfig.Builder newBuilder() {
    return new HoodieFunctionalIndexConfig.Builder();
  }

  public static class Builder {
    private final HoodieFunctionalIndexConfig functionalIndexConfig = new HoodieFunctionalIndexConfig();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.functionalIndexConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder fromProperties(Properties props) {
      this.functionalIndexConfig.getProps().putAll(props);
      return this;
    }

    public Builder fromIndexConfig(HoodieFunctionalIndexConfig functionalIndexConfig) {
      this.functionalIndexConfig.getProps().putAll(functionalIndexConfig.getProps());
      return this;
    }

    public Builder withIndexName(String indexName) {
      functionalIndexConfig.setValue(INDEX_NAME, indexName);
      return this;
    }

    public Builder withIndexType(String indexType) {
      functionalIndexConfig.setValue(INDEX_TYPE, indexType);
      return this;
    }

    public Builder withIndexFunction(String indexFunction) {
      functionalIndexConfig.setValue(INDEX_FUNCTION, indexFunction);
      return this;
    }

    public HoodieFunctionalIndexConfig build() {
      functionalIndexConfig.setDefaults(HoodieFunctionalIndexConfig.class.getName());
      return functionalIndexConfig;
    }
  }

  public static HoodieFunctionalIndexConfig copy(HoodieFunctionalIndexConfig functionalIndexConfig) {
    return newBuilder().fromIndexConfig(functionalIndexConfig).build();
  }

  public static HoodieFunctionalIndexConfig merge(HoodieFunctionalIndexConfig functionalIndexConfig1, HoodieFunctionalIndexConfig functionalIndexConfig2) {
    return newBuilder().fromIndexConfig(functionalIndexConfig1).fromIndexConfig(functionalIndexConfig2).build();
  }

  public static HoodieFunctionalIndexConfig fromIndexDefinition(HoodieFunctionalIndexDefinition indexDefinition) {
    return newBuilder().withIndexName(indexDefinition.getIndexName())
        .withIndexType(indexDefinition.getIndexType())
        .withIndexFunction(indexDefinition.getIndexFunction())
        .build();
  }

  public String getIndexName() {
    return getString(INDEX_NAME);
  }

  public String getIndexType() {
    return getString(INDEX_TYPE);
  }

  public String getIndexFunction() {
    return getString(INDEX_FUNCTION);
  }

  public boolean isIndexUsingBloomFilter() {
    return getIndexType().equalsIgnoreCase(MetadataPartitionType.BLOOM_FILTERS.name());
  }

  public boolean isIndexUsingLucene() {
    return getIndexType().equalsIgnoreCase(SecondaryIndexType.LUCENE.name());
  }

  public boolean isIndexUsingColumnStats() {
    return getIndexType().equalsIgnoreCase(MetadataPartitionType.COLUMN_STATS.name());
  }

  public boolean isIndexUsingRecordIndex() {
    return getIndexType().equalsIgnoreCase(MetadataPartitionType.RECORD_INDEX.name());
  }
}
