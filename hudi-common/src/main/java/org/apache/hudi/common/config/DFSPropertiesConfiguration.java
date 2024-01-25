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
import org.apache.hudi.io.storage.HoodieLocation;
import org.apache.hudi.io.storage.HoodieStorage;
import org.apache.hudi.io.storage.HoodieStorageUtils;

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
import java.util.HashSet;
import java.util.Set;

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
public class DFSPropertiesConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(DFSPropertiesConfiguration.class);

  public static final String DEFAULT_PROPERTIES_FILE = "hudi-defaults.conf";
  public static final String CONF_FILE_DIR_ENV_NAME = "HUDI_CONF_DIR";
  public static final String DEFAULT_CONF_FILE_DIR = "file:/etc/hudi/conf";
  public static final HoodieLocation DEFAULT_LOCATION = new HoodieLocation(
      DEFAULT_CONF_FILE_DIR + "/" + DEFAULT_PROPERTIES_FILE);

  // props read from hudi-defaults.conf
  private static TypedProperties GLOBAL_PROPS = loadGlobalProps();

  @Nullable
  private final Configuration hadoopConfig;

  private HoodieLocation mainFileLocation;

  // props read from user defined configuration file or input stream
  private final HoodieConfig hoodieConfig;

  // Keep track of files visited, to detect loops
  private final Set<String> visitedFilePaths;

  public DFSPropertiesConfiguration(@Nonnull Configuration hadoopConf, @Nonnull HoodieLocation fileLocation) {
    this.hadoopConfig = hadoopConf;
    this.mainFileLocation = fileLocation;
    this.hoodieConfig = new HoodieConfig();
    this.visitedFilePaths = new HashSet<>();
    addPropsFromFile(fileLocation);
  }

  public DFSPropertiesConfiguration() {
    this.hadoopConfig = null;
    this.mainFileLocation = null;
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
      try (BufferedReader br = new BufferedReader(new InputStreamReader(configFile.openStream()))) {
        conf.addPropsFromStream(br, new HoodieLocation(configFile.toURI()));
        return conf.getProps();
      } catch (URISyntaxException e) {
        throw new HoodieException(String.format("Provided props file url is invalid %s", configFile), e);
      } catch (IOException ioe) {
        throw new HoodieIOException(
            String.format("Failed to read %s from class loader", DEFAULT_PROPERTIES_FILE), ioe);
      }
    }
    // Try loading the external config file from local file system
    Option<HoodieLocation> defaultConfLocation = getConfLocationFromEnv();
    if (defaultConfLocation.isPresent()) {
      conf.addPropsFromFile(defaultConfLocation.get());
    } else {
      try {
        conf.addPropsFromFile(DEFAULT_LOCATION);
      } catch (Exception e) {
        LOG.warn("Cannot load default config file: " + DEFAULT_LOCATION, e);
      }
    }
    return conf.getProps();
  }

  public static void refreshGlobalProps() {
    GLOBAL_PROPS = loadGlobalProps();
  }

  public static void clearGlobalProps() {
    GLOBAL_PROPS = new TypedProperties();
  }

  /**
   * Add properties from external configuration files.
   *
   * @param fileLocation file location for configuration file.
   */
  public void addPropsFromFile(HoodieLocation fileLocation) {
    if (visitedFilePaths.contains(fileLocation.toString())) {
      throw new IllegalStateException("Loop detected; file " + fileLocation + " already referenced");
    }

    HoodieStorage storage = HoodieStorageUtils.getHoodieStorage(
        fileLocation,
        Option.ofNullable(hadoopConfig).orElseGet(Configuration::new)
    );

    try {
      if (fileLocation.equals(DEFAULT_LOCATION) && !storage.exists(fileLocation)) {
        LOG.warn("Properties file " + fileLocation + " not found. Ignoring to load props file");
        return;
      }
    } catch (IOException ioe) {
      throw new HoodieIOException("Cannot check if the properties file exist: " + fileLocation, ioe);
    }

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(storage.open(fileLocation)))) {
      visitedFilePaths.add(fileLocation.toString());
      addPropsFromStream(reader, fileLocation);
    } catch (IOException ioe) {
      LOG.error("Error reading in properties from dfs from file " + fileLocation);
      throw new HoodieIOException("Cannot read properties from dfs from file " + fileLocation, ioe);
    }
  }

  /**
   * Add properties from buffered reader.
   *
   * @param reader Buffered Reader
   * @throws IOException
   */
  public void addPropsFromStream(BufferedReader reader, HoodieLocation cfgFileLocation) throws IOException {
    try {
      reader.lines().forEach(line -> {
        if (!isValidLine(line)) {
          return;
        }
        String[] split = splitProperty(line);
        if (line.startsWith("include=") || line.startsWith("include =")) {
          HoodieLocation providedPath = new HoodieLocation(split[1]);
          HoodieStorage providedStorage = HoodieStorageUtils.getHoodieStorage(split[1], hadoopConfig);
          // In the case that only filename is provided, assume it's in the same directory.
          if ((!providedPath.isAbsolute() || StringUtils.isNullOrEmpty(providedStorage.getScheme()))
              && cfgFileLocation != null) {
            providedPath = new HoodieLocation(cfgFileLocation.getParent(), split[1]);
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

  public static TypedProperties getGlobalProps() {
    final TypedProperties globalProps = new TypedProperties();
    globalProps.putAll(GLOBAL_PROPS);
    return globalProps;
  }

  // test only
  public static TypedProperties addToGlobalProps(String key, String value) {
    GLOBAL_PROPS.put(key, value);
    return GLOBAL_PROPS;
  }

  public TypedProperties getProps() {
    return new TypedProperties(hoodieConfig.getProps());
  }

  public TypedProperties getProps(boolean includeGlobalProps) {
    return new TypedProperties(hoodieConfig.getProps(includeGlobalProps));
  }

  private static Option<HoodieLocation> getConfLocationFromEnv() {
    String confDir = System.getenv(CONF_FILE_DIR_ENV_NAME);
    if (confDir == null) {
      LOG.warn("Cannot find " + CONF_FILE_DIR_ENV_NAME + ", please set it as the dir of " + DEFAULT_PROPERTIES_FILE);
      return Option.empty();
    }
    if (StringUtils.isNullOrEmpty(URI.create(confDir).getScheme())) {
      confDir = "file://" + confDir;
    }
    return Option.of(new HoodieLocation(confDir + File.separator + DEFAULT_PROPERTIES_FILE));
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
