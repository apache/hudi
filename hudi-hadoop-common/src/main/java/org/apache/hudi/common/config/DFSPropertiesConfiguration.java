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

package org.apache.hudi.common.config;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A simplified versions of Apache commons - PropertiesConfiguration, that supports limited field types and hierarchical
 * configurations within the same folder as the root file.
 *
 * Includes denoted by the same include=filename.properties syntax, with relative path from root file's folder. Lines
 * beginning with '#' are ignored as comments. Final values for properties are resolved by the order in which they are
 * specified in the files, with included files treated as if they are inline.
 *
 * Note: Not reusing commons-configuration since it has too many conflicting runtime deps.
 */
public class DFSPropertiesConfiguration extends PropertiesConfig {

  private static final Logger LOG = LoggerFactory.getLogger(DFSPropertiesConfiguration.class);

  public static final String DEFAULT_PROPERTIES_FILE = "hudi-defaults.conf";
  public static final String CONF_FILE_DIR_ENV_NAME = "HUDI_CONF_DIR";
  public static final String DEFAULT_CONF_FILE_DIR = "file:/etc/hudi/conf";
  public static final StoragePath DEFAULT_PATH = new StoragePath(
      DEFAULT_CONF_FILE_DIR, DEFAULT_PROPERTIES_FILE);

  /**
   * Holder class for lazy initialization of global properties.
   * Initialized on first access to avoid exceptions during class loading.
   */
  private static class GlobalPropsHolder {
    static final AtomicReference<TypedProperties> INSTANCE = new AtomicReference<>(null);
  }

  @Nullable
  private final Configuration hadoopConfig;

  private final StoragePath mainFilePath;

  // props read from user defined configuration file or input stream
  private final HoodieConfig hoodieConfig;

  // Keep track of files visited, to detect loops
  private final Set<String> visitedFilePaths;

  public DFSPropertiesConfiguration(@Nonnull Configuration hadoopConf, @Nonnull StoragePath filePath) {
    this.hadoopConfig = hadoopConf;
    this.mainFilePath = filePath;
    this.hoodieConfig = new HoodieConfig();
    this.visitedFilePaths = new HashSet<>();
    addPropsFromFile(filePath);
  }

  public DFSPropertiesConfiguration() {
    this.hadoopConfig = new Configuration();
    this.mainFilePath = null;
    this.hoodieConfig = new HoodieConfig();
    this.visitedFilePaths = new HashSet<>();
  }

  /**
   * Load global props from hudi-defaults.conf which is under class loader or CONF_FILE_DIR_ENV_NAME.
   * @return Typed Properties
   */
  public static TypedProperties loadGlobalProps() {
    DFSPropertiesConfiguration conf = new DFSPropertiesConfiguration();

    // First try loading the external config file from class loader
    URL configFile = Thread.currentThread().getContextClassLoader().getResource(DEFAULT_PROPERTIES_FILE);
    if (configFile != null) {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(configFile.openStream(), StandardCharsets.UTF_8))) {
        conf.addPropsFromStream(br, new StoragePath(configFile.toURI()));
        return conf.getProps();
      } catch (URISyntaxException e) {
        throw new HoodieException(String.format("Provided props file url is invalid %s", configFile), e);
      } catch (IOException ioe) {
        throw new HoodieIOException(
            String.format("Failed to read %s from class loader", DEFAULT_PROPERTIES_FILE), ioe);
      }
    }
    // Try loading the external config file from local file system
    try {
      conf.addPropsFromFile(DEFAULT_PATH);
    } catch (Exception e) {
      LOG.warn("Cannot load default config file: {}", DEFAULT_PATH, e);
    }
    Option<StoragePath> defaultConfPath = getConfPathFromEnv();
    if (defaultConfPath.isPresent() && !defaultConfPath.get().equals(DEFAULT_PATH)) {
      conf.addPropsFromFile(defaultConfPath.get());
    }
    return conf.getProps();
  }

  public static void refreshGlobalProps() {
    TypedProperties fresh = loadGlobalProps();
    GlobalPropsHolder.INSTANCE.set(fresh);
  }

  public static void clearGlobalProps() {
    GlobalPropsHolder.INSTANCE.set(new TypedProperties());
  }

  /**
   * Add properties from external configuration files.
   *
   * @param filePath file path for configuration file.
   */
  public void addPropsFromFile(StoragePath filePath) {
    if (visitedFilePaths.contains(filePath.toString())) {
      throw new IllegalStateException("Loop detected; file " + filePath + " already referenced");
    }

    HoodieStorage storage = new HoodieHadoopStorage(
        filePath,
        HadoopFSUtils.getStorageConf(Option.ofNullable(hadoopConfig).orElseGet(Configuration::new))
    );

    try {
      if (filePath.equals(DEFAULT_PATH) && !storage.exists(filePath)) {
        LOG.debug("Properties file {} not found. Ignoring to load props file", filePath);
        return;
      }
    } catch (IOException ioe) {
      throw new HoodieIOException("Cannot check if the properties file exist: " + filePath, ioe);
    }

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(storage.open(filePath), StandardCharsets.UTF_8))) {
      visitedFilePaths.add(filePath.toString());
      addPropsFromStream(reader, filePath);
    } catch (IOException ioe) {
      LOG.error("Error reading in properties from dfs from file " + filePath);
      throw new HoodieIOException("Cannot read properties from dfs from file " + filePath, ioe);
    }
  }

  /**
   * Add properties from buffered reader.
   *
   * @param reader Buffered Reader
   * @throws IOException
   */
  public void addPropsFromStream(BufferedReader reader, StoragePath cfgFilePath) throws IOException {
    try {
      reader.lines().forEach(line -> {
        if (!isValidLine(line)) {
          return;
        }
        String[] split = splitProperty(line);
        if (line.startsWith("include=") || line.startsWith("include =")) {
          StoragePath providedPath = new StoragePath(split[1]);
          HoodieStorage providedStorage = HoodieStorageUtils.getStorage(
              split[1], HadoopFSUtils.getStorageConf(hadoopConfig));
          // In the case that only filename is provided, assume it's in the same directory.
          if ((!providedPath.isAbsolute() || StringUtils.isNullOrEmpty(providedStorage.getScheme()))
              && cfgFilePath != null) {
            providedPath = new StoragePath(cfgFilePath.getParent(), split[1]);
          }
          addPropsFromFile(providedPath);
        } else {
          hoodieConfig.setValue(split[0], split[1]);
        }
      });

    } finally {
      reader.close();
    }
  }

  @Override
  public TypedProperties getGlobalProperties() {
    return getGlobalProps();
  }

  public static TypedProperties getGlobalProps() {
    TypedProperties props = GlobalPropsHolder.INSTANCE.get();

    if (props == null) {
      TypedProperties loaded = loadGlobalProps();
      if (GlobalPropsHolder.INSTANCE.compareAndSet(null, loaded)) {
        LOG.info("Loaded global properties from configuration");
      }
      props = GlobalPropsHolder.INSTANCE.get();
    }

    final TypedProperties copy = new TypedProperties();
    copy.putAll(props);
    return copy;
  }

  // test only
  public static TypedProperties addToGlobalProps(String key, String value) {
    if (GlobalPropsHolder.INSTANCE.get() == null) {
      getGlobalProps();
    }
    TypedProperties current = GlobalPropsHolder.INSTANCE.get();
    TypedProperties updated = new TypedProperties();
    updated.putAll(current);
    updated.put(key, value);
    GlobalPropsHolder.INSTANCE.set(updated);
    return updated;
  }

  public TypedProperties getProps() {
    return TypedProperties.copy(hoodieConfig.getProps());
  }

  public TypedProperties getProps(boolean includeGlobalProps) {
    return TypedProperties.copy(hoodieConfig.getProps(includeGlobalProps));
  }

  private static Option<StoragePath> getConfPathFromEnv() {
    String confDir = System.getenv(CONF_FILE_DIR_ENV_NAME);
    if (confDir == null) {
      LOG.debug("Environment variable " + CONF_FILE_DIR_ENV_NAME + ", not set. If desired, set it to the folder containing: " + DEFAULT_PROPERTIES_FILE);
      return Option.empty();
    }
    if (StringUtils.isNullOrEmpty(URI.create(confDir).getScheme())) {
      confDir = "file://" + confDir;
    }
    return Option.of(new StoragePath(confDir + File.separator + DEFAULT_PROPERTIES_FILE));
  }

  private String[] splitProperty(String line) {
    line = line.replaceAll("\\s+", " ");
    String delimiter = line.contains("=") ? "=" : " ";
    int ind = line.indexOf(delimiter);
    String k = line.substring(0, ind).trim();
    String v = line.substring(ind + 1).trim();
    return new String[] {k, v};
  }

  private boolean isValidLine(String line) {
    ValidationUtils.checkArgument(line != null, "passed line is null");
    if (line.startsWith("#") || line.equals("")) {
      return false;
    }
    return line.contains("=") || line.matches(".*\\s.*");
  }
}
