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

package org.apache.hudi.utils;

import org.apache.hudi.common.config.*;
import org.apache.hudi.common.config.ConfigGroups.Names;
import org.apache.hudi.common.config.ConfigGroups.SubGroupNames;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.configuration.FlinkOptions;

import net.steppschuh.markdowngenerator.link.Link;
import net.steppschuh.markdowngenerator.list.ListBuilder;
import net.steppschuh.markdowngenerator.rule.HorizontalRule;
import net.steppschuh.markdowngenerator.table.Table;
import net.steppschuh.markdowngenerator.table.TableRow;
import net.steppschuh.markdowngenerator.text.Text;
import net.steppschuh.markdowngenerator.text.emphasis.BoldText;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.description.HtmlFormatter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.reflections.Reflections;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.apache.hudi.common.config.ConfigGroups.SubGroupNames.NONE;
import static org.reflections.ReflectionUtils.getAllFields;
import static org.reflections.ReflectionUtils.withTypeAssignableTo;

/**
 * 1. Get all subclasses of {@link HoodieConfig}
 * 2. For each subclass, get all fields of type {@link ConfigProperty}.
 * 3. Add config key and description to table row.
 */
public class HoodieConfigDocGenerator {

  private static final Logger LOG = LogManager.getLogger(HoodieConfigDocGenerator.class);
  private static final String NEWLINE = "\n";
  private static final String LINE_BREAK = "<br></br>\n";
  private static final String DOUBLE_NEWLINE = "\n\n";
  private static final String NOTE_ON_NA = new StringBuilder().append(":::note \n").append("In the tables below **(N/A)** means there is no default value set\n").append(":::\n").toString();
  private static final String ALL_CONFIGS_PAGE_SUMMARY = "This page covers the different ways of configuring " +
          "your job to write/read Hudi tables. " +
          "At a high level, you can control behaviour at few levels.";
  private static final String BASIC_CONFIGS_PAGE_SUMMARY = "This page covers the basic configurations you may use to " +
          "write/read Hudi tables. This page only features a subset of the most frequently used configurations. For a " +
          "full list of all configs, please visit the [All Configurations](configurations.md) page.";
  private static final String FLINK_CONFIG_CLASS_NAME = "org.apache.hudi.configuration.FlinkOptions";
  private static final String ALL_CONFIGS_PATH = "/tmp/configurations.md";
  private static final String BASIC_CONFIGS_PATH = "/tmp/basic_configurations.md";
  private static final String EXTERNALIZED_CONFIGS = "## Externalized Config File\n" +
          "Instead of directly passing configuration settings to every Hudi job, you can also centrally set them in a configuration\n" +
          "file `hudi-defaults.conf`. By default, Hudi would load the configuration file under `/etc/hudi/conf` directory. You can\n" +
          "specify a different configuration directory location by setting the `HUDI_CONF_DIR` environment variable. This can be\n" +
          "useful for uniformly enforcing repeated configs (like Hive sync or write/index tuning), across your entire data lake.";
  private static final String DEFAULT_FOOTER_MARKUP = new StringBuilder().append(NEWLINE).append(new HorizontalRule(3)).append(DOUBLE_NEWLINE).toString();
  private static final Integer DEFAULT_CONFIG_GROUP_HEADING_LEVEL = 2;
  private static final Integer DEFAULT_CONFIG_PARAM_HEADING_LEVEL = 3;
  private static final TableRow<String> DEFAULT_TABLE_HEADER_ROW = new TableRow<>(new ArrayList<>(Arrays.asList("Config Name", "Default", "Description")));

  public static void main(String[] args) {
    Reflections reflections = new Reflections("org.apache.hudi");
    // Create a Treemap of [config group/subgroup/commonconfigs/configclass] -> [configclass markup]
    NavigableMap<ConfigClassMeta, ConfigClassMarkups> configClassTreeMap = new TreeMap<>(getConfigClassMetaComparator());
    initConfigClassTreeMap(reflections.getSubTypesOf(HoodieConfig.class), configClassTreeMap);
    buildConfigMarkup(configClassTreeMap);
    initAndBuildSparkConfigMarkup(configClassTreeMap);
    generateAllConfigurationPages(configClassTreeMap);
    generateBasicConfigurationPages(configClassTreeMap);
  }

  private static StringBuilder generateExternalizedConfigs() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(EXTERNALIZED_CONFIGS);
    stringBuilder.append(DOUBLE_NEWLINE);
    return stringBuilder;
  }

  /**
   * Generated main content headings for every config group except for those from the exclusion list.
   */
  private static void generateMainHeadings(ListBuilder builder, EnumSet<Names> exclusionList) {
    EnumSet<Names> filteredSet = EnumSet.noneOf(Names.class);
    EnumSet.allOf(Names.class).stream().filter(g -> (!exclusionList.contains(g))).forEach(g -> filteredSet.add(g));
    filteredSet.forEach(groupName -> builder.append(
            new Link(new BoldText(groupName.name),
                    "#" + groupName.name())
                    + ": " + ConfigGroups.getDescription(groupName)));
  }

  /**
   * Returns the header meta for the all configs doc page. This will be a .mdx page.
   */
  private static void generateAllConfigsHeader(StringBuilder builder) {
    /*
      ---
      title: Configurations
      keywords: [configurations, default, flink options, spark, configs, parameters]
      permalink: /docs/configurations.html
      summary: This section offers an overview of tools available to operate an ecosystem of Hudi
      toc_min_heading_level: 2
      toc_max_heading_level: 4
      last_modified_at: 2019-12-30T15:59:57-04:00
      ---
     */
    LocalDateTime now = LocalDateTime.now();
    builder.append(new HorizontalRule()).append(NEWLINE)
            .append("title: ").append("All Configurations").append(NEWLINE)
            .append("keywords: [ configurations, default, flink options, spark, configs, parameters ] ").append(NEWLINE)
            .append("permalink: /docs/configurations.html").append(NEWLINE)
            .append("summary: " + ALL_CONFIGS_PAGE_SUMMARY).append(NEWLINE)
            .append("toc_min_heading_level: 2").append(NEWLINE)
            .append("toc_max_heading_level: 4").append(NEWLINE)
            .append("last_modified_at: " + DateTimeFormatter.ISO_DATE_TIME.format(now)).append(NEWLINE)
            .append(new HorizontalRule()).append(NEWLINE)
            .append(DOUBLE_NEWLINE);
    // Description
    builder.append(ALL_CONFIGS_PAGE_SUMMARY).append(DOUBLE_NEWLINE);
  }

  /**
   * Returns the header meta for the all configs doc page. This will be a .mdx page.
   */
  private static void generateBasicConfigsHeader(StringBuilder builder) {
    /*
      ---
      title: Basic Configurations
      summary: This page covers the basic configurations you may use to write/read Hudi tables. This page only
      features a subset of the most frequently used configurations. For a full list of all configs, please visit the
      [All Configurations](configurations.md) page.
      last_modified_at: 2019-12-30T15:59:57-04:00
      ---
     */
    LocalDateTime now = LocalDateTime.now();
    builder.append(new HorizontalRule()).append(NEWLINE)
            .append("title: ").append("Basic Configurations").append(NEWLINE)
            .append("summary: " + BASIC_CONFIGS_PAGE_SUMMARY).append(NEWLINE)
            .append("last_modified_at: " + DateTimeFormatter.ISO_DATE_TIME.format(now)).append(NEWLINE)
            .append(new HorizontalRule()).append(NEWLINE)
            .append(DOUBLE_NEWLINE);
    // Description
    builder.append(BASIC_CONFIGS_PAGE_SUMMARY).append(DOUBLE_NEWLINE);
  }

  /**
   * Generate a ConfigTableRow for the given config
   */
  private static ConfigTableRow generateConfigTableRow(Class subType, Field field, Object object) {
    try {
      ConfigProperty cfgProperty = (ConfigProperty) field.get(object);
      List<String> columns = new ArrayList<>();

      if (StringUtils.isNullOrEmpty(cfgProperty.doc())) {
        LOG.warn("Found empty or null description for config class = "
                + subType.getName()
                + " for param = "
                + field.getName());
      }

      // Config Key
      String configKeyWithAnchorLink = "[" + cfgProperty.key() + "](#" + cfgProperty.key().replace(" ", "-").replace(".", "") + ")";
      columns.add(configKeyWithAnchorLink);

      // Default value
      Object defaultValue = cfgProperty.hasDefaultValue() ? cfgProperty.defaultValue() : (cfgProperty.hasInferFunction() ? "" : null);
      if (defaultValue != null) {
        columns.add(defaultValue + " ");
      } else {
        columns.add("(N/A)");
      }
      boolean isConfigRequired = (defaultValue == null);

      // Description
      String configParam = "`Config Param: " + field.getName() + "`";
      String description = StringUtils.isNullOrEmpty(cfgProperty.doc()) ? "" : cfgProperty.doc().replaceAll("[\\t\\n\\r]+", " ").replaceAll("&", "&amp;").replaceAll("\\|", " &#124; ").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll("&lt;ul&gt;", "<ul>").replaceAll("&lt;/ul&gt;", "</ul>").replaceAll("&lt;li&gt;", "<li>").replaceAll("&lt;/li&gt;", "</li>");

      // First version
      String versionInfo = "";
      if (cfgProperty.getSinceVersion().isPresent()) {
        String sinceVersion = "<br />`Since Version: " + cfgProperty.getSinceVersion().get() + "`";
        String deprecatedVersion = "";
        if (cfgProperty.getDeprecatedVersion().isPresent()) {
          deprecatedVersion = "<br />`Deprecated since: " + cfgProperty.getDeprecatedVersion().get() + "`";
        }
        versionInfo = sinceVersion + deprecatedVersion;
      }
      columns.add(description + "<br />" + configParam + versionInfo);

      return new ConfigTableRow(cfgProperty.key(), new TableRow<>(columns), isConfigRequired, cfgProperty.isAdvanced());
    } catch (IllegalAccessException e) {
      LOG.error("Error while getting field through reflection for config class: " + subType.getName(), e);
      throw new IllegalArgumentException("Error while getting field through reflection for config class: " + subType.getName(), e);
    }
  }

  /**
   * Returns the markup heading string for given heading size
   */
  private static String getHeadingSizeMarkup(int headingSize) {
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < headingSize; i++) {
      stringBuilder.append("#");
    }
    stringBuilder.append(" ");
    return stringBuilder.toString();
  }

  /**
   * Returns the formatted summary for main Config group.
   */
  private static String generateConfigGroupSummary(String friendlyName, String anchorString, String description, int headingSize) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(getHeadingSizeMarkup(headingSize))
            .append(friendlyName)
            .append(" {" + "#").append(anchorString).append("}")
            .append(NEWLINE)
            .append(description)
            .append(DOUBLE_NEWLINE);
    return stringBuilder.toString();
  }

  private static String generateConfigClassParam(String subTypeName) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("`")
            .append(new Text("Config Class"))
            .append("`")
            .append(": ")
            .append(subTypeName).append(LINE_BREAK);
    return stringBuilder.toString();
  }

  /**
   * Generates the basic/advanced configs from the given list of configs
   */
  private static String generateBasicOrAdvancedConfigsFrom(List<ConfigTableRow> configs, String configClassPrefix, boolean basicConfigs) {
    StringBuilder stringBuilder = new StringBuilder();
    String anchorString;
    String configTableHeading;
    if (basicConfigs) {
      anchorString = configClassPrefix + "-basic-configs";
      configTableHeading = "Basic Configs";
    } else {
      anchorString = configClassPrefix + "-advanced-configs";
      configTableHeading = "Advanced Configs";
    }
    Table.Builder configsTable = new Table.Builder();
    configsTable.addRow(DEFAULT_TABLE_HEADER_ROW);
    for (ConfigTableRow config : configs) {
      configsTable.addRow(config.getColumns());
    }
    stringBuilder.append(DOUBLE_NEWLINE)
            .append("[" + new BoldText(configTableHeading) + "]")
            .append("(" + "#").append(anchorString).append(")")
            .append(NEWLINE)
            .append(DOUBLE_NEWLINE);
    stringBuilder.append(configsTable.build());
    return stringBuilder.toString();
  }

  /**
   * Comparator to be used when ordering the config classes. Used to sort the tree map based on these columns in the
   * order - groupname, subgroupname (reverse order), areCommonConfigs (reverse order) and the config class name. We
   * want to list all groups with no subgroups first. Followed by groups with subgroups and among them list the class
   * that has common configs first. The group name and subgroup name are based on ENUM position instead of actual
   * string.
   */
  public static Comparator<ConfigClassMeta> getConfigClassMetaComparator() {
    return Comparator.comparing(ConfigClassMeta::getGroupName)
            .thenComparing(ConfigClassMeta::getSubGroupName, Comparator.reverseOrder())
            .thenComparing(ConfigClassMeta::areCommonConfigs, Comparator.reverseOrder())
            .thenComparing(ConfigClassMeta::getClassName);
  }

  /**
   * Initializes the tree map with empty objects for config markup sections.
   *
   * @param subTypes
   * @param configClassTreeMap
   */
  private static void initConfigClassTreeMap(Set<Class<? extends HoodieConfig>> subTypes, NavigableMap<ConfigClassMeta, ConfigClassMarkups> configClassTreeMap) {
    for (Class<? extends HoodieConfig> subType : subTypes) {
      // sub-heading using the annotation
      ConfigClassProperty configClassProperty = subType.getAnnotation(ConfigClassProperty.class);
      LOG.info(subType.getName());
      try {
        if (configClassProperty != null) {
          ConfigClassMeta configClassMeta = new ConfigClassMeta(configClassProperty.groupName(), configClassProperty.subGroupName(), configClassProperty.areCommonConfigs(), subType);
          configClassTreeMap.put(configClassMeta, new ConfigClassMarkups());
        } else {
          LOG.error("FATAL error Please add `ConfigClassProperty` annotation for " + subType.getName());
        }
      } catch (Exception e) {
        LOG.error("FATAL error while processing config class: " + subType.getName(), e);
      }
    }
  }

  /**
   * Builds the config markups for all Config classes (Flink included) except Spark based ones.
   * This can be reused later to generate config tables in individual doc pages.
   */
  private static void buildConfigMarkup(NavigableMap<ConfigClassMeta, ConfigClassMarkups> configClassTreeMap) {
    // generate Docs from the config classes
    Names prevGroupName = Names.ENVIRONMENT_CONFIG;
    SubGroupNames prevSubGroupName = NONE;
    String prevGroupSummary = null;
    String prevSubGroupSummary = null;
    int configParamHeadingLevel = DEFAULT_CONFIG_PARAM_HEADING_LEVEL;
    Set<ConfigClassMeta> keySet = configClassTreeMap.keySet();

    for (ConfigClassMeta configClassMetaInfo : keySet) {
      ConfigClassMarkups configClassMarkup = configClassTreeMap.get(configClassMetaInfo);
      Class<? extends HoodieConfig> subType = configClassMetaInfo.subType;
      ConfigClassProperty configClassProperty = subType.getAnnotation(ConfigClassProperty.class);
      Names groupName = configClassProperty.groupName();

      /*
      We need to handle an exception for the ConfigGroup SPARK_DATASOURCE since HoodiePreCommitValidatorConfig that
      belongs to this group extends HoodieConfig whereas other config classes in this group dont extend HoodieConfig.
      They are handled in initAndBuildSparkConfigMarkup(..) method differently */
      if (groupName != prevGroupName && groupName != Names.SPARK_DATASOURCE) {
        prevGroupSummary = generateConfigGroupSummary(groupName.name, groupName.name(), ConfigGroups.getDescription(groupName), DEFAULT_CONFIG_GROUP_HEADING_LEVEL);
        prevGroupName = groupName;
      }
      LOG.info("Processing params for config class: " + subType.getName() + " " + configClassProperty.name()
              + " " + configClassProperty.description());
      configClassMarkup.topLevelGroupSummary = prevGroupSummary;
      if (configClassMetaInfo.subGroupName == NONE) {
        configParamHeadingLevel = DEFAULT_CONFIG_PARAM_HEADING_LEVEL;
        prevSubGroupSummary = null;
      } else if (configClassMetaInfo.subGroupName == prevSubGroupName) {
        // Continuation of more HoodieConfig classes that are part of the same subgroup
        configParamHeadingLevel = DEFAULT_CONFIG_PARAM_HEADING_LEVEL + 1;
      } else {
        // This is a new valid Subgroup encountered. Add description for the subgroup itself.
        String configSubGroupSummary = generateConfigGroupSummary(
                configClassMetaInfo.subGroupName.name,
                configClassMetaInfo.subGroupName.name(),
                configClassProperty.subGroupName().getDescription(),
                DEFAULT_CONFIG_GROUP_HEADING_LEVEL + 1);
        prevSubGroupSummary = configSubGroupSummary;
        configParamHeadingLevel = DEFAULT_CONFIG_PARAM_HEADING_LEVEL + 1;
      }
      prevSubGroupName = configClassMetaInfo.subGroupName;
      configClassMarkup.topLevelSubGroupSummary = prevSubGroupSummary;
      String anchorString = configClassProperty.name().replace(" ", "-");
      String configClassSummary = generateConfigGroupSummary(
              configClassProperty.name(),
              anchorString,
              configClassProperty.description(),
              configParamHeadingLevel);
      configClassSummary.concat(generateConfigClassParam(subType.getName()));
      configClassMarkup.configClassSummary = configClassSummary;

      // Special casing Flink Configs since the class does not use ConfigClassProperty
      // Also, we need to split Flink Options into Flink Read Options, Write Options...
      if (subType.getName().equals(FLINK_CONFIG_CLASS_NAME)) {
        populateFlinkConfigs(subType, configClassMarkup, anchorString);
      } else {
        populateConfigs(subType, null, configClassMarkup, anchorString);
      }
    }
  }

  /**
   * Populates the configs for Spark section. This needs to be special cased since the class does not extend
   * HoodieConfig and also does not use ConfigClassProperty.
   */
  private static void initAndBuildSparkConfigMarkup(NavigableMap<ConfigClassMeta, ConfigClassMarkups> configClassTreeMap) {
    Names groupName = Names.SPARK_DATASOURCE;
    // Add entries for spark related sections next
    for (Object sparkConfigObject : HoodieSparkConfigs.getSparkConfigObjects()) {
      String configClassName = HoodieSparkConfigs.name(sparkConfigObject);
      ConfigClassMeta configClassMetaInfo = new ConfigClassMeta(groupName, NONE, false, configClassName);
      ConfigClassMarkups configClassMarkup = new ConfigClassMarkups();
      configClassMarkup.topLevelGroupSummary = generateConfigGroupSummary(groupName.name, groupName.name(), ConfigGroups.getDescription(groupName), DEFAULT_CONFIG_GROUP_HEADING_LEVEL);

      LOG.info("Processing params for config class: " + configClassName + " desc: " + HoodieSparkConfigs.description(sparkConfigObject));

      String anchorString = configClassName.replace(" ", "-");
      String configClassSummary = generateConfigGroupSummary(
              configClassName,
              anchorString,
              HoodieSparkConfigs.description(sparkConfigObject),
              DEFAULT_CONFIG_PARAM_HEADING_LEVEL);
      configClassSummary.concat(generateConfigClassParam(HoodieSparkConfigs.className()));
      configClassMarkup.configClassSummary = configClassSummary;
      populateConfigs(sparkConfigObject.getClass(), sparkConfigObject, configClassMarkup, anchorString);
      configClassTreeMap.put(configClassMetaInfo, configClassMarkup);
    }
  }

  /**
   * Populates the configs for Flink section. This is special cased since the class does not use ConfigClassProperty.
   */
  private static void populateFlinkConfigs(Class subType, ConfigClassMarkups configClassMarkup, String configClassAnchorString) {
    try {
      List<ConfigTableRow> basicConfigs = new ArrayList();
      List<ConfigTableRow> advancedConfigs = new ArrayList();
      Set<Field> fields = getAllFields(FlinkOptions.class, withTypeAssignableTo(ConfigOption.class));
      for (Field field : fields) {
        ConfigOption cfgProperty = (ConfigOption) field.get(null);
        List<String> columns = new ArrayList<>();
        String description = new HtmlFormatter().format(cfgProperty.description());
        if (description.isEmpty()) {
          LOG.warn("Found empty or null description for config class = "
                  + subType.getName()
                  + " for param = "
                  + field.getName());
        }
        // Config key
        String configKeyWithAnchorLink = "[" + cfgProperty.key() + "](#" + cfgProperty.key().replace(" ", "-").replace(".", "") + ")";
        columns.add(configKeyWithAnchorLink);

        // Default value
        Object defaultValue = cfgProperty.hasDefaultValue() ? cfgProperty.defaultValue() : null;
        if (defaultValue != null) {
          columns.add(defaultValue + " ");
        } else {
          columns.add("(N/A)");
        }
        boolean isConfigRequired = (defaultValue == null);

        // Description
        String configParam = " `Config Param: " + field.getName() + "`";
        String desc = StringUtils.isNullOrEmpty(description) ? "" : description.replaceAll("[\\t\\n\\r]+", " ");
        columns.add(desc + "<br />" + configParam);

        ConfigTableRow configRow = new ConfigTableRow(cfgProperty.key(), new TableRow<>(columns), isConfigRequired);

        if (field.isAnnotationPresent(AdvancedConfig.class)) {
          advancedConfigs.add(configRow);
        } else {
          basicConfigs.add(configRow);
        }
      }
      // sort the configs based on config key prefix and add to the configParamsBuilder
      basicConfigs.sort(Comparator.comparing(ConfigTableRow::isConfigRequired).reversed()
              .thenComparing(ConfigTableRow::getConfigKey));
      advancedConfigs.sort(Comparator.comparing(ConfigTableRow::isConfigRequired).reversed()
              .thenComparing(ConfigTableRow::getConfigKey));
      if (!basicConfigs.isEmpty()) {
        configClassMarkup.basicConfigs = generateBasicOrAdvancedConfigsFrom(basicConfigs, configClassAnchorString, true);
      }
      if (!advancedConfigs.isEmpty()) {
        configClassMarkup.advancedConfigs = generateBasicOrAdvancedConfigsFrom(advancedConfigs, configClassAnchorString, false);
      }
    } catch (IllegalAccessException e) {
      LOG.error("Error while getting field through reflection for config class: " + subType.getName(), e);
    }
  }

  /**
   * Generates the basic configs and advanced configs tables for the given Config Class.
   */
  private static void populateConfigs(Class subType, Object object, ConfigClassMarkups configClassMarkup, String configClassAnchorString) {
    Set<Field> fields = getAllFields(subType, withTypeAssignableTo(ConfigProperty.class));
    List<ConfigTableRow> basicConfigs = new ArrayList<>();
    List<ConfigTableRow> advancedConfigs = new ArrayList<>();
    for (Field field : fields) {
      field.setAccessible(true);
      ConfigTableRow tableRow = generateConfigTableRow(subType, field, object);
      if (tableRow.isConfigAdvanced()) {
        advancedConfigs.add(tableRow);
      } else {
        basicConfigs.add(tableRow);
      }
    }
    // sort the configs based on config key prefix and add to the configParamsBuilder
    basicConfigs.sort(Comparator.comparing(ConfigTableRow::isConfigRequired).reversed()
            .thenComparing(ConfigTableRow::getConfigKey));
    advancedConfigs.sort(Comparator.comparing(ConfigTableRow::isConfigRequired).reversed()
            .thenComparing(ConfigTableRow::getConfigKey));
    if (!basicConfigs.isEmpty()) {
      configClassMarkup.basicConfigs = generateBasicOrAdvancedConfigsFrom(basicConfigs, configClassAnchorString, true);
    }
    if (!advancedConfigs.isEmpty()) {
      configClassMarkup.advancedConfigs = generateBasicOrAdvancedConfigsFrom(advancedConfigs, configClassAnchorString, false);
    }
  }

  /**
   * Generates the markdown file for all configurations.
   */
  private static void generateAllConfigurationPages(NavigableMap<ConfigClassMeta, ConfigClassMarkups> configClassTreeMap) {
    LOG.info("Generating markdown file");
    StringBuilder mainDocBuilder = new StringBuilder();
    generateAllConfigsHeader(mainDocBuilder);
    ListBuilder contentTableBuilder = new ListBuilder();
    generateMainHeadings(contentTableBuilder, EnumSet.noneOf(Names.class));
    mainDocBuilder.append(contentTableBuilder.build()).append(DOUBLE_NEWLINE);
    mainDocBuilder.append(NOTE_ON_NA).append(NEWLINE);
    mainDocBuilder.append(generateExternalizedConfigs());
    Set<ConfigClassMeta> keySet = configClassTreeMap.keySet();

    Names prevGroupName = Names.ENVIRONMENT_CONFIG;
    SubGroupNames prevSubGroupName = NONE;
    for (ConfigClassMeta configClassMetaInfo : keySet) {
      ConfigClassMarkups configClassMarkup = configClassTreeMap.get(configClassMetaInfo);
      if(configClassMetaInfo.groupName != prevGroupName){
        mainDocBuilder.append(configClassMarkup.topLevelGroupSummary);
        prevGroupName = configClassMetaInfo.groupName;
      }
      if(configClassMetaInfo.subGroupName != prevSubGroupName){
        if (configClassMetaInfo.subGroupName != NONE && configClassMarkup.topLevelSubGroupSummary != null){
          mainDocBuilder.append(NEWLINE).append(configClassMarkup.topLevelSubGroupSummary);
        }
        prevSubGroupName = configClassMetaInfo.subGroupName;
      }
      mainDocBuilder.append(NEWLINE).append(configClassMarkup.configClassSummary);
      if (configClassMarkup.basicConfigs != null) {
        mainDocBuilder.append(configClassMarkup.basicConfigs);
      }
      if (configClassMarkup.advancedConfigs != null) {
        mainDocBuilder.append(configClassMarkup.advancedConfigs);
      }
      mainDocBuilder.append(DEFAULT_FOOTER_MARKUP);
    }
    try {
      Files.write(Paths.get(ALL_CONFIGS_PATH), mainDocBuilder.toString().getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      LOG.error("Error while writing to markdown file ", e);
    }
  }

  /**
   * Generates the markdown file for basic configurations page.
   */
  private static void generateBasicConfigurationPages(NavigableMap<ConfigClassMeta, ConfigClassMarkups> configClassTreeMap) {
    LOG.info("Generating markdown file");
    StringBuilder mainDocBuilder = new StringBuilder();
    generateBasicConfigsHeader(mainDocBuilder);
    ListBuilder contentTableBuilder = new ListBuilder();
    // Build a list of all Config groups that have basic configs
    EnumSet<Names> inclusionList = EnumSet.noneOf(Names.class);
    // Iterate the Treemap and get all config groups and classes that have basic configs.
    Set<ConfigClassMeta> keySet = configClassTreeMap.keySet();

    Names prevGroupName = Names.ENVIRONMENT_CONFIG;
    SubGroupNames prevSubGroupName = NONE;
    StringBuilder stringBuilder = new StringBuilder();
    for (ConfigClassMeta configClassMetaInfo : keySet) {
      ConfigClassMarkups configClassMarkup = configClassTreeMap.get(configClassMetaInfo);
      if (configClassMarkup.basicConfigs == null) {
        continue;
      }
      inclusionList.add(configClassMetaInfo.groupName);
      if(configClassMetaInfo.groupName != prevGroupName){
        stringBuilder.append(configClassMarkup.topLevelGroupSummary);
        prevGroupName = configClassMetaInfo.groupName;
      }
      if(configClassMetaInfo.subGroupName != prevSubGroupName){
        if (configClassMetaInfo.subGroupName != NONE && configClassMarkup.topLevelSubGroupSummary != null){
          stringBuilder.append(NEWLINE).append(configClassMarkup.topLevelSubGroupSummary);
        }
        prevSubGroupName = configClassMetaInfo.subGroupName;
      }
      stringBuilder.append(NEWLINE).append(configClassMarkup.configClassSummary);
      stringBuilder.append(NEWLINE).append(configClassMarkup.basicConfigs);
      stringBuilder.append(DEFAULT_FOOTER_MARKUP);
    }
    generateMainHeadings(contentTableBuilder, EnumSet.complementOf(inclusionList));
    mainDocBuilder.append(contentTableBuilder.build()).append(DOUBLE_NEWLINE);
    mainDocBuilder.append(NOTE_ON_NA).append(NEWLINE);
    mainDocBuilder.append(stringBuilder);
    try {
      Files.write(Paths.get(BASIC_CONFIGS_PATH), mainDocBuilder.toString().getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      LOG.error("Error while writing to markdown file ", e);
    }
  }


  /**
   * Class for storing info about each config within a HoodieConfig class subtype. We want to know if a config is
   * required or not. And also store the config key along with other attributes. We can use this info to pull up all
   * required configs and also sort configs based on prefix for better readability.
   **/
  static class ConfigTableRow {
    String configKey;
    boolean isConfigRequired;
    TableRow<String> columns;
    boolean isAdvanced = false;

    ConfigTableRow(String configKey, TableRow<String> columns, boolean isConfigRequired) {
      this.configKey = configKey;
      this.columns = columns;
      this.isConfigRequired = isConfigRequired;
    }

    ConfigTableRow(String configKey, TableRow<String> columns, boolean isConfigRequired, boolean isAdvanced) {
      this.configKey = configKey;
      this.columns = columns;
      this.isConfigRequired = isConfigRequired;
      this.isAdvanced = isAdvanced;
    }

    public boolean isConfigRequired() {
      return isConfigRequired;
    }

    public boolean isConfigAdvanced() {
      return isAdvanced;
    }

    public TableRow<String> getColumns() {
      return columns;
    }

    public String getConfigKey() {
      return configKey;
    }
  }

  /**
   * Class for storing meta info about each HoodiConfig class subtype and also Spark datasource classes. This class has
   * info on the group name, subgroup name if any, indication of whether this subtype has common configs for that
   * subgroup and the subtype itself.
   */
  static class ConfigClassMeta {
    Names groupName;
    SubGroupNames subGroupName;
    boolean hasCommonConfigs;
    /*
    Only one of the following can be null since we are using this to track a subclass of HoodieConfig and also
    DataSource classes which dont extend HoodieConfig.
    */
    Class<? extends HoodieConfig> subType; // Can be null for Spark datasource classes.
    String sparkConfigObjectName; // Can be null for flink and general HoodieConfig classes

    ConfigClassMeta(Names groupName, SubGroupNames subGroupName, boolean hasCommonConfigs, Class<? extends HoodieConfig> subType) {
      this.groupName = groupName;
      this.subGroupName = subGroupName;
      this.hasCommonConfigs = hasCommonConfigs;
      this.subType = subType;
    }

    ConfigClassMeta(Names groupName, SubGroupNames subGroupName, boolean hasCommonConfigs, String sparkConfigObjectName) {
      this.groupName = groupName;
      this.subGroupName = subGroupName;
      this.hasCommonConfigs = hasCommonConfigs;
      this.sparkConfigObjectName = sparkConfigObjectName;
    }

    public Names getGroupName() {
      return groupName;
    }

    public SubGroupNames getSubGroupName() {
      return subGroupName;
    }

    public boolean areCommonConfigs() {
      return hasCommonConfigs;
    }

    public String getClassName() {
      return (subType != null ? subType.getName() : sparkConfigObjectName);
    }
  }

  /**
   * Class for tracking the markdown string of individual config class's summary, basic configs and advanced configs.
   */
  static class ConfigClassMarkups {
    String topLevelGroupSummary;
    String topLevelSubGroupSummary;
    String configClassSummary;
    String basicConfigs;
    String advancedConfigs;
  }

}
