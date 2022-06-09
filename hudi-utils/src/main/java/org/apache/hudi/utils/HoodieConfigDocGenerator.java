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
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

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

  public static void main(String[] args) {
    Reflections reflections = new Reflections("org.apache.hudi");
    Set<Class<? extends HoodieConfig>> subTypes = reflections.getSubTypesOf(HoodieConfig.class);
    // Top heading
    StringBuilder mainDocBuilder = new StringBuilder();
    generateHeader(mainDocBuilder);

    ListBuilder contentTableBuilder = new ListBuilder();
    Map<ConfigGroups.Names, StringBuilder> contentMap = generateContentTableAndMainHeadings(contentTableBuilder);

    // Special casing Spark Configs since the class does not extend HoodieConfig
    // and also does not use ConfigClassProperty
    populateSparkConfigs(contentMap);

    // Automated: Scan through all HoodieConfig superclasses using reflection
    for (Class<? extends HoodieConfig> subType : subTypes) {
      // sub-heading using the annotation
      ConfigClassProperty configGroupProperty = subType.getAnnotation(ConfigClassProperty.class);
      try {
        if (configGroupProperty != null) {
          StringBuilder configParamsBuilder = contentMap.get(configGroupProperty.groupName());
          LOG.info("Processing params for config class: " + subType.getName() + " " + configGroupProperty.name()
            + " " + configGroupProperty.description());
          configParamsBuilder.append("### ").append(configGroupProperty.name())
              .append(" {" + "#").append(configGroupProperty.name().replace(" ", "-")).append("}")
              .append(DOUBLE_NEWLINE);
          configParamsBuilder.append(configGroupProperty.description()).append(DOUBLE_NEWLINE);

          configParamsBuilder
              .append("`")
              .append(new Text("Config Class"))
              .append("`")
              .append(": ")
              .append(subType.getName()).append(LINE_BREAK);

          // Special casing Flink Configs since the class does not use ConfigClassProperty
          // Also, we need to split Flink Options into Flink Read Options, Write Options...
          if (subType.getName().equals(FLINK_CONFIG_CLASS_NAME)) {
            generateFlinkConfigMarkup(subType, configParamsBuilder);
          } else {
            Set<Field> fields = getAllFields(subType, withTypeAssignableTo(ConfigProperty.class));
            for (Field field : fields) {
              generateConfigMarkup(subType, field, null, configParamsBuilder);
            }
          }
        } else {
          LOG.error("FATAL error Please add `ConfigClassProperty` annotation for " + subType.getName());
        }
      } catch (Exception e) {
        LOG.error("FATAL error while processing config class: " + subType.getName(), e);
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

  private static void generateHeader(StringBuilder builder) {
    /*
      ---
      title: Configurations
      keywords: [configurations, default, flink options, spark, configs, parameters]
      permalink: /docs/configurations.html
      summary: This section offers an overview of tools available to operate an ecosystem of Hudi
      toc: true
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
      stringBuilder.append("## ")
          .append(groupName.name)
          .append(" {" + "#").append(groupName.name()).append("}")
          .append(NEWLINE)
          .append(ConfigGroups.getDescription(groupName))
          .append(DOUBLE_NEWLINE);
      contentMap.put(groupName, stringBuilder);
    });
    return contentMap;
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
      for (Field field : hardcodedFields) {
        field.setAccessible(true);
        generateConfigMarkup(sparkConfigObject.getClass(), field, sparkConfigObject, configParamsBuilder);
      }
    }
  }

  private static void generateFlinkConfigMarkup(Class subType, StringBuilder configParamsBuilder) {
      try {
        Set<Field> fields = getAllFields(FlinkOptions.class, withTypeAssignableTo(ConfigOption.class));
        for (Field field : fields) {
          ConfigOption cfgProperty = (ConfigOption) field.get(null);
          String description = new HtmlFormatter().format(cfgProperty.description());
          if (description.isEmpty()) {
            LOG.warn("Found empty or null description for config class = "
                + subType.getName()
                + " for param = "
                + field.getName());
          }
          // Config Header
          configParamsBuilder.append("> ").append("#### ").append(new Text(cfgProperty.key())).append(NEWLINE);

          // Description
          configParamsBuilder
              .append("> ")
              .append(description)
              .append(LINE_BREAK);

          // Default value
          addDefaultValue(configParamsBuilder, cfgProperty.hasDefaultValue() ? cfgProperty.defaultValue() : null);

          // Config param name
          generateConfigKeyValue(configParamsBuilder, true, "Config Param", field.getName());

          configParamsBuilder
              .append(NEWLINE)
              .append(new HorizontalRule(3))
              .append(DOUBLE_NEWLINE);
        }
      } catch (IllegalAccessException e) {
        LOG.error("Error while getting field through reflection for config class: " + subType.getName(), e);
      }
  }

  private static void generateConfigMarkup(Class subType, Field field, Object object, StringBuilder configParamsBuilder) {
    try {
      ConfigProperty cfgProperty = (ConfigProperty) field.get(object);
      if (StringUtils.isNullOrEmpty(cfgProperty.doc())) {
        LOG.warn("Found empty or null description for config class = "
            + subType.getName()
            + " for param = "
            + field.getName());
      }

      // Config Header
      configParamsBuilder.append("> ").append("#### ").append(new Text(cfgProperty.key())).append(NEWLINE);

      // Description
      String description = StringUtils.isNullOrEmpty(cfgProperty.doc()) ? "" : cfgProperty.doc();
      configParamsBuilder
          .append("> ")
          .append(description)
          .append(LINE_BREAK);

      // Default value
      addDefaultValue(configParamsBuilder, cfgProperty.hasDefaultValue() ? cfgProperty.defaultValue() : null);

      // Config param name
      generateConfigKeyValue(configParamsBuilder, true, "Config Param", field.getName());

      // First version
      if (cfgProperty.getSinceVersion().isPresent()) {
        generateConfigKeyValue(configParamsBuilder, true, "Since Version", cfgProperty.getSinceVersion().get());
      }

      if (cfgProperty.getDeprecatedVersion().isPresent()) {
        generateConfigKeyValue(configParamsBuilder, true, "Deprecated Version", cfgProperty.getDeprecatedVersion().get());
      }

      configParamsBuilder
          .append(NEWLINE)
          .append(new HorizontalRule(3))
          .append(DOUBLE_NEWLINE);
    } catch (IllegalAccessException e) {
      LOG.error("Error while getting field through reflection for config class: " + subType.getName(), e);
    }
  }

  private static void addDefaultValue(StringBuilder builder, @Nullable Object defaultValue) {
    generateConfigKeyValue(builder, false, "Default Value",
        (defaultValue != null) ? defaultValue + " (Optional)" : "N/A (Required)");
  }

  private static void generateConfigKeyValue(StringBuilder builder,
                                             boolean shouldHighlight,
                                             String key,
                                             Object value) {
    if (shouldHighlight) {
      builder
          .append("> ")
          .append("`")
          .append(new Text(key))
          .append(": ")
          .append(new Text(value))
          .append("`")
          .append(LINE_BREAK);
    } else {
      builder
          .append("> ")
          .append(new BoldText(key))
          .append(": ")
          .append(new Text(value))
          .append(LINE_BREAK);
    }
  }
}
