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

import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.configuration.FlinkOptions;

import net.steppschuh.markdowngenerator.link.Link;
import net.steppschuh.markdowngenerator.list.ListBuilder;
import net.steppschuh.markdowngenerator.rule.HorizontalRule;
import net.steppschuh.markdowngenerator.text.Text;
import net.steppschuh.markdowngenerator.text.emphasis.BoldText;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.description.HtmlFormatter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.reflections.ReflectionUtils;
import org.reflections.Reflections;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  private static final String SUMMARY = "This page covers the different ways of configuring " +
      "your job to write/read Hudi tables. " +
      "At a high level, you can control behaviour at few levels.";
  private static final String FLINK_CONFIG_CLASS_NAME = "org.apache.hudi.configuration.FlinkOptions";
  private static final String CONFIG_PATH = "/tmp/configurations.md";
  private static final String EXTERNALIZED_CONFIGS = "## Externalized Config File\n" +
          "Instead of directly passing configuration settings to every Hudi job, you can also centrally set them in a configuration\n" +
          "file `hudi-default.conf`. By default, Hudi would load the configuration file under `/etc/hudi/conf` directory. You can\n" +
          "specify a different configuration directory location by setting the `HUDI_CONF_DIR` environment variable. This can be\n" +
          "useful for uniformly enforcing repeated configs (like Hive sync or write/index tuning), across your entire data lake.";
  private static final Integer DEFAULT_CONFIG_GROUP_HEADING_LEVEL = 2;
  private static final Integer DEFAULT_CONFIG_PARAM_HEADING_LEVEL = 3;

  public static void main(String[] args) {
    Reflections reflections = new Reflections("org.apache.hudi");
    // Scan and collect meta info of all HoodieConfig superclasses by using reflection
    List<HoodieConfigClassMetaInfo> hoodieConfigClassMetaInfos = getSortedListOfHoodieConfigClassMetaInfo(reflections.getSubTypesOf(HoodieConfig.class));

    // Top heading
    StringBuilder mainDocBuilder = new StringBuilder();
    generateHeader(mainDocBuilder);

    ListBuilder contentTableBuilder = new ListBuilder();
    Map<ConfigGroups.Names, StringBuilder> contentMap = generateContentTableAndMainHeadings(contentTableBuilder);

    // Special casing Spark Configs since the class does not extend HoodieConfig
    // and also does not use ConfigClassProperty
    populateSparkConfigs(contentMap);

    // generate Docs from the config classes
    ConfigGroups.SubGroupNames prevSubGroupName = NONE;
    boolean isPartOfSubGroup = false;
    int configParamHeadingLevel = DEFAULT_CONFIG_PARAM_HEADING_LEVEL;
    for (HoodieConfigClassMetaInfo configClassMetaInfo: hoodieConfigClassMetaInfos) {
      Class<? extends HoodieConfig> subType = configClassMetaInfo.subType;
      ConfigClassProperty configClassProperty = subType.getAnnotation(ConfigClassProperty.class);
      StringBuilder groupOrSubGroupStringBuilder = contentMap.get(configClassProperty.groupName());
      LOG.info("Processing params for config class: " + subType.getName() + " " + configClassProperty.name()
              + " " + configClassProperty.description());
      if (configClassMetaInfo.subGroupName == NONE){
        isPartOfSubGroup = false;
        configParamHeadingLevel = DEFAULT_CONFIG_PARAM_HEADING_LEVEL;
      } else if (configClassMetaInfo.subGroupName == prevSubGroupName) {
        // Continuation of more HoodieConfig classes that are part of the same subgroup
        isPartOfSubGroup = true;
        groupOrSubGroupStringBuilder = new StringBuilder();
        configParamHeadingLevel = DEFAULT_CONFIG_PARAM_HEADING_LEVEL + 1;
      } else if (configClassMetaInfo.hasCommonConfigs) {
        // This is a new valid Subgroup encountered. Add description for the subgroup.
        isPartOfSubGroup = true;
        groupOrSubGroupStringBuilder = new StringBuilder();
        generateConfigGroupSummary(groupOrSubGroupStringBuilder,
                configClassMetaInfo.subGroupName.name,
                configClassMetaInfo.subGroupName.name(),
                configClassProperty.subGroupName().getDescription(),
                DEFAULT_CONFIG_GROUP_HEADING_LEVEL + 1);
        configParamHeadingLevel = DEFAULT_CONFIG_PARAM_HEADING_LEVEL + 1;
      }
      prevSubGroupName = configClassMetaInfo.subGroupName;
      generateConfigGroupSummary(groupOrSubGroupStringBuilder,
              configClassProperty.name(),
              configClassProperty.name().replace(" ", "-"),
              configClassProperty.description(),
              configParamHeadingLevel);
      groupOrSubGroupStringBuilder
              .append("`")
              .append(new Text("Config Class"))
              .append("`")
              .append(": ")
              .append(subType.getName()).append(LINE_BREAK);

      // Special casing Flink Configs since the class does not use ConfigClassProperty
      // Also, we need to split Flink Options into Flink Read Options, Write Options...
      if (subType.getName().equals(FLINK_CONFIG_CLASS_NAME)) {
        generateFlinkConfigMarkup(subType, groupOrSubGroupStringBuilder);
      } else {
        generateAllOtherConfigs(subType, isPartOfSubGroup, groupOrSubGroupStringBuilder);
        if (isPartOfSubGroup) {
          // If the config class is part of a subgroup, close the string builder for subgroup and append it to the main group builder.
          contentMap.get(configClassProperty.groupName()).append(groupOrSubGroupStringBuilder.toString());
        }
      }
    }

    try {
      LOG.info("Generating markdown file");
      mainDocBuilder.append(contentTableBuilder.build()).append(DOUBLE_NEWLINE);
      mainDocBuilder.append(generateExternalizedConfigs());
      contentMap.forEach((k, v) -> mainDocBuilder.append(v));
      Files.write(Paths.get(CONFIG_PATH), mainDocBuilder.toString().getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      LOG.error("Error while writing to markdown file ", e);
    }
  }

  private static List<HoodieConfigClassMetaInfo> getSortedListOfHoodieConfigClassMetaInfo(Set<Class<? extends HoodieConfig>> subTypes) {
    // Scan and collect meta info of all HoodieConfig superclasses by using reflection
    List<HoodieConfigClassMetaInfo> hoodieConfigClassMetaInfos = new ArrayList<>();
    for (Class<? extends HoodieConfig> subType : subTypes) {
      // sub-heading using the annotation
      ConfigClassProperty configClassProperty = subType.getAnnotation(ConfigClassProperty.class);
      try{
        if (configClassProperty != null) {
          hoodieConfigClassMetaInfos.add(new HoodieConfigClassMetaInfo(configClassProperty.groupName(), configClassProperty.subGroupName(), configClassProperty.areCommonConfigs(), subType));
        } else {
          LOG.error("FATAL error Please add `ConfigClassProperty` annotation for " + subType.getName());
        }
      } catch (Exception e) {
        LOG.error("FATAL error while processing config class: " + subType.getName(), e);
      }
    }

    // Now sort them based on these columns in the order - groupname, subgroupname (reverse order) and areCommonConfigs (reverse order)
    // We want to list all groups with no subgroups first. Followed by groups with subgroups and among them list the
    // class that has common configs first.
    hoodieConfigClassMetaInfos.sort(Comparator.comparing(HoodieConfigClassMetaInfo::getGroupName)
            .thenComparing(HoodieConfigClassMetaInfo::getSubGroupName, Comparator.reverseOrder())
            .thenComparing(HoodieConfigClassMetaInfo::areCommonConfigs, Comparator.reverseOrder()));
    return hoodieConfigClassMetaInfos;
  }

  private static void generateHeader(StringBuilder builder) {
    /*
      ---
      title: Configurations
      keywords: [configurations, default, flink options, spark, configs, parameters]
      permalink: /docs/configurations.html
      summary: This section offers an overview of tools available to operate an ecosystem of Hudi
      toc: true
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
        .append("summary: " + SUMMARY).append(NEWLINE)
        .append("toc: true").append(NEWLINE)
        .append("toc_min_heading_level: 2").append(NEWLINE)
        .append("toc_max_heading_level: 4").append(NEWLINE)
        .append("last_modified_at: " + DateTimeFormatter.ISO_DATE_TIME.format(now)).append(NEWLINE)
        .append(new HorizontalRule())
        .append(DOUBLE_NEWLINE);
    // Description
    builder.append(SUMMARY).append(DOUBLE_NEWLINE);
  }

  private static Map<ConfigGroups.Names, StringBuilder> generateContentTableAndMainHeadings(ListBuilder builder) {
    EnumSet.allOf(ConfigGroups.Names.class).forEach(groupName -> builder.append(
        new Link(new BoldText(groupName.name),
            "#" + groupName.name())
            + ": " + ConfigGroups.getDescription(groupName)));
    Map<ConfigGroups.Names, StringBuilder> contentMap = new LinkedHashMap<>();
    EnumSet.allOf(ConfigGroups.Names.class).forEach(groupName -> {
      StringBuilder stringBuilder = new StringBuilder();
      generateConfigGroupSummary(stringBuilder, groupName.name, groupName.name(), ConfigGroups.getDescription(groupName), DEFAULT_CONFIG_GROUP_HEADING_LEVEL);
      contentMap.put(groupName, stringBuilder);
    });
    return contentMap;
  }

  private static void generateConfigGroupSummary(StringBuilder stringBuilder, String friendlyName, String groupName, String description, int headingSize) {
    stringBuilder.append(getHeadingSizeMarkup(headingSize))
            .append(friendlyName)
            .append(" {" + "#").append(groupName).append("}")
            .append(NEWLINE)
            .append(description)
            .append(DOUBLE_NEWLINE);
  }

  private static String getHeadingSizeMarkup(int headingSize){
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < headingSize; i++) {
      stringBuilder.append("#");
    }
    stringBuilder.append(" ");
    return stringBuilder.toString();
  }
  private static StringBuilder generateExternalizedConfigs() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(EXTERNALIZED_CONFIGS);
    stringBuilder.append(DOUBLE_NEWLINE);
    return stringBuilder;
  }

  private static void populateSparkConfigs(Map<ConfigGroups.Names, StringBuilder> contentMap) {
    StringBuilder configParamsBuilder = contentMap.get(ConfigGroups.Names.SPARK_DATASOURCE);

    for (Object sparkConfigObject : HoodieSparkConfigs.getSparkConfigObjects()) {
      String configName = HoodieSparkConfigs.name(sparkConfigObject);
      LOG.info("Processing params for config class: " + configName + " desc: " + HoodieSparkConfigs.description(sparkConfigObject));

      configParamsBuilder.append("### ").append(configName)
          .append(" {" + "#").append(configName.replace(" ", "-")).append("}")
          .append(DOUBLE_NEWLINE);
      configParamsBuilder.append(HoodieSparkConfigs.description(sparkConfigObject)).append(DOUBLE_NEWLINE);

      configParamsBuilder
          .append("`")
          .append(new Text("Config Class"))
          .append("`")
          .append(": ")
          .append(HoodieSparkConfigs.className()).append(LINE_BREAK);


      Set<Field> hardcodedFields = ReflectionUtils.getAllFields(sparkConfigObject.getClass(), withTypeAssignableTo(ConfigProperty.class));

      List<ConfigMarkup> allConfigs = new ArrayList<>();
      for (Field field : hardcodedFields) {
        field.setAccessible(true);
        ConfigMarkup configMarkup = generateConfigMarkup(sparkConfigObject.getClass(), field, sparkConfigObject, DEFAULT_CONFIG_PARAM_HEADING_LEVEL);
        allConfigs.add(configMarkup);
      }
      // sort the configs based on config key prefix and add to the configParamsBuilder
      allConfigs.sort(Comparator.comparing(ConfigMarkup::isConfigRequired).reversed()
              .thenComparing(ConfigMarkup::getConfigKey));
      allConfigs.forEach(cfg -> configParamsBuilder.append(cfg.configMarkupString));
    }
  }

  private static void generateFlinkConfigMarkup(Class subType, StringBuilder configParamsBuilder) {
      try {
        List<ConfigMarkup> allConfigs = new ArrayList();
        Set<Field> fields = getAllFields(FlinkOptions.class, withTypeAssignableTo(ConfigOption.class));
        for (Field field : fields) {
          StringBuilder tmpConfigParamBuilder = new StringBuilder();
          ConfigOption cfgProperty = (ConfigOption) field.get(null);
          String description = new HtmlFormatter().format(cfgProperty.description());
          if (description.isEmpty()) {
            LOG.warn("Found empty or null description for config class = "
                + subType.getName()
                + " for param = "
                + field.getName());
          }
          // Config Header
          tmpConfigParamBuilder.append("> ").append("#### ").append(new Text(cfgProperty.key())).append(NEWLINE);

          // Description
          tmpConfigParamBuilder
              .append("> ")
              .append(description)
              .append(LINE_BREAK);

          // Default value
          Object defaultValue = cfgProperty.hasDefaultValue() ? cfgProperty.defaultValue() : null;
          addDefaultValue(tmpConfigParamBuilder, defaultValue);
          boolean isConfigRequired = (defaultValue == null);

          // TODO: Custom config tags like "Doc on Default Value:" cannot be added for Flink.
          //  ConfigOption is a Flink class. In order to support custom config apis like getDocOnDefaultValue
          //  this class needs to be wrapped in Hudi first.

          // Config param name
          generateConfigKeyValue(tmpConfigParamBuilder, "Config Param", field.getName());

          tmpConfigParamBuilder
              .append(NEWLINE)
              .append(new HorizontalRule(3))
              .append(DOUBLE_NEWLINE);

          ConfigMarkup configMarkup = new ConfigMarkup(cfgProperty.key(), isConfigRequired, tmpConfigParamBuilder.toString());
          allConfigs.add(configMarkup);
        }

        // sort the configs based on config key prefix and add to the configParamsBuilder
        allConfigs.sort(Comparator.comparing(ConfigMarkup::isConfigRequired).reversed()
                .thenComparing(ConfigMarkup::getConfigKey));
        allConfigs.forEach(cfg -> configParamsBuilder.append(cfg.configMarkupString));
      } catch (IllegalAccessException e) {
        LOG.error("Error while getting field through reflection for config class: " + subType.getName(), e);
      }
  }

  private static void generateAllOtherConfigs(Class<? extends HoodieConfig> subType, boolean isPartOfSubGroup, StringBuilder stringBuilder) {
    Set<Field> fields = getAllFields(subType, withTypeAssignableTo(ConfigProperty.class));
    List<ConfigMarkup> allConfigs = new ArrayList<>();
    for (Field field : fields) {
      ConfigMarkup configMarkup = generateConfigMarkup(subType, field, null, isPartOfSubGroup? DEFAULT_CONFIG_PARAM_HEADING_LEVEL + 1 : DEFAULT_CONFIG_PARAM_HEADING_LEVEL);
      allConfigs.add(configMarkup);
    }
    // sort the configs based on config key prefix and add to the configParamsBuilder
    allConfigs.sort(Comparator.comparing(ConfigMarkup::isConfigRequired).reversed()
            .thenComparing(ConfigMarkup::getConfigKey));
    for (ConfigMarkup cfg: allConfigs) {
      stringBuilder.append(cfg.configMarkupString);
    }
  }

  private static ConfigMarkup generateConfigMarkup(Class subType, Field field, Object object, int headingLevel) {
    try {
      StringBuilder configParamsBuilder = new StringBuilder();
      ConfigProperty cfgProperty = (ConfigProperty) field.get(object);
      if (StringUtils.isNullOrEmpty(cfgProperty.doc())) {
        LOG.warn("Found empty or null description for config class = "
                + subType.getName()
                + " for param = "
                + field.getName());
      }

      // Config Header
      configParamsBuilder.append("> ").append(getHeadingSizeMarkup(headingLevel)).append(new Text(cfgProperty.key())).append(NEWLINE);

      // Description
      String description = StringUtils.isNullOrEmpty(cfgProperty.doc()) ? "" : cfgProperty.doc();
      configParamsBuilder
              .append("> ")
              .append(description)
              .append(LINE_BREAK);

      // Default value
      Object defaultValue = cfgProperty.hasDefaultValue() ? cfgProperty.defaultValue() : (cfgProperty.hasInferFunction() ? "" : null );
      addDefaultValue(configParamsBuilder, defaultValue);
      boolean isConfigRequired = (defaultValue == null);

      // Note on Default value
      if (StringUtils.nonEmpty(cfgProperty.getDocOnDefaultValue()) && !cfgProperty.getDocOnDefaultValue().equals(StringUtils.EMPTY_STRING)) {
        addDocOnDefaultValue(configParamsBuilder, cfgProperty.getDocOnDefaultValue());
      }

      // Config param name
      generateConfigKeyValue(configParamsBuilder, "Config Param", field.getName());

      // First version
      if (cfgProperty.getSinceVersion().isPresent()) {
        generateConfigKeyValue(configParamsBuilder, "Since Version", String.valueOf(cfgProperty.getSinceVersion().get()));
      }

      if (cfgProperty.getDeprecatedVersion().isPresent()) {
        generateConfigKeyValue(configParamsBuilder, "Deprecated Version", String.valueOf(cfgProperty.getDeprecatedVersion().get()));
      }

      configParamsBuilder
              .append(NEWLINE)
              .append(new HorizontalRule(3))
              .append(DOUBLE_NEWLINE);
      return new ConfigMarkup(cfgProperty.key(), isConfigRequired, configParamsBuilder.toString());
    } catch (IllegalAccessException e) {
      LOG.error("Error while getting field through reflection for config class: " + subType.getName(), e);
      throw new IllegalArgumentException("Error while getting field through reflection for config class: " + subType.getName(), e);
    }
  }

  private static void addDefaultValue(StringBuilder builder, @Nullable Object defaultValue) {
    boolean isRequired = false;
    if (defaultValue != null) {
      builder
              .append("> ")
              .append("`")
              .append(new Text("Default Value"))
              .append(": ")
              .append(new Text(defaultValue + " (Optional)"))
              .append("`")
              .append(LINE_BREAK);
    } else {
      builder
              .append("> ")
              .append("`")
              .append(new Text("Default Value"))
              .append(": N/A ")
              .append("`")
              .append(new BoldText("(Required)"))
              .append(LINE_BREAK);
      // TODO: Use this to highlight required configs blue in a later release. We are not enabling this now since we need
      //  to review required configs in Hudi repo first towards simplification.
      //.append(new BoldText("<span style={{color: '#0db1f9'}}>(Required)</span>"))
    }
  }

  private static void addDocOnDefaultValue(StringBuilder builder, String value) {
    generateConfigKeyValue(builder, "Note on Default Value", value);
  }

  private static void generateConfigKeyValue(StringBuilder builder,
                                             String key,
                                             String value) {
      builder
          .append("> ")
          .append("`")
          .append(new Text(key))
          .append(": ")
          .append(value)
          .append("`")
          .append(LINE_BREAK);
  }

  /**
  Class for storing info about each config within a HoodieConfig class subtype. We want to know if a config is required or not. And also store the config key
  along with the markup. We can use this info to pull up all required configs and also sort configs based on prefix
  for better readability.
  **/

  static class ConfigMarkup {
    String configKey;
    boolean isConfigRequired;
    String configMarkupString;

    ConfigMarkup(String configKey, boolean isConfigRequired, String configMarkupString){
      this.configKey = configKey;
      this.isConfigRequired = isConfigRequired;
      this.configMarkupString = configMarkupString;
    }

    public boolean isConfigRequired() {
      return isConfigRequired;
    }

    public String getConfigKey() {
      return configKey;
    }
  }

  /**
   * Class for storing meta info about each HoodiConfig class subtype. This class has info on the group name, subgroup
   * name if any, indication of whether this subtype has common configs for that subgroup and the subtype itself.
   */
  static class HoodieConfigClassMetaInfo {
    ConfigGroups.Names groupName;
    ConfigGroups.SubGroupNames subGroupName;
    boolean hasCommonConfigs;
    Class<? extends HoodieConfig> subType;
    HoodieConfigClassMetaInfo(ConfigGroups.Names groupName, ConfigGroups.SubGroupNames subGroupName, boolean  hasCommonConfigs, Class<? extends HoodieConfig> subType) {
      this.groupName = groupName;
      this.subGroupName = subGroupName;
      this.hasCommonConfigs = hasCommonConfigs;
      this.subType = subType;
    }

    public ConfigGroups.Names getGroupName() {
      return groupName;
    }

    public ConfigGroups.SubGroupNames getSubGroupName() {
      return subGroupName;
    }

    public boolean areCommonConfigs() {
      return hasCommonConfigs;
    }
  }
}
