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

import org.apache.hudi.common.config.ConfigGroupProperty;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.util.StringUtils;

import net.steppschuh.markdowngenerator.rule.HorizontalRule;
import net.steppschuh.markdowngenerator.text.Text;
import net.steppschuh.markdowngenerator.text.heading.Heading;
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
  private static final String LINE_BREAK = "<br>\n";
  private static final String DOUBLE_NEWLINE = "\n\n";
  private static final String SUMMARY = "This page covers the different ways of configuring " +
      "your job to write/read Hudi tables. " +
      "At a high level, you can control behaviour at few levels.";

  public static void main(String[] args) {
    Reflections reflections = new Reflections("org.apache.hudi");
    Set<Class<? extends HoodieConfig>> subTypes = reflections.getSubTypesOf(HoodieConfig.class);
    // Top heading
    StringBuilder configDocBuilder = new StringBuilder();
    generateHeader(configDocBuilder);

    for (Class<? extends HoodieConfig> subType : subTypes) {
      // sub-heading using the annotation
      ConfigGroupProperty configGroupProperty = subType.getAnnotation(ConfigGroupProperty.class);
      if (configGroupProperty != null) {
        LOG.info("Processing params for config class: " + subType.getName() + " " + configGroupProperty.name()
            + " " + configGroupProperty.description());
        configDocBuilder.append("## ").append(configGroupProperty.name())
            .append(DOUBLE_NEWLINE);
        configDocBuilder.append(configGroupProperty.description()).append("\n\n");
      } else {
        configDocBuilder.append(new Heading(subType.getSimpleName(), 2)).append("\n\n");
        LOG.warn("Please add annotation ConfigGroupProperty to config class: " + subType.getName());
      }

      Set<Field> fields = getAllFields(subType, withTypeAssignableTo(ConfigProperty.class));
      for (Field field : fields) {
        ConfigProperty obj = null;
        try {
          ConfigProperty cfgProperty = (ConfigProperty) field.get(obj);
          if (StringUtils.isNullOrEmpty(cfgProperty.doc())) {
            LOG.warn("Found empty or null description for config class = "
                + subType.getName()
                + " for param = "
                + field.getName());
          }

          configDocBuilder.append("> ").append("### ").append(new Text(cfgProperty.key())).append(NEWLINE);

          configDocBuilder
              .append("> ")
              .append("`")
              .append("Default Value")
              .append(": ")
              .append(cfgProperty.hasDefaultValue() ? cfgProperty.defaultValue() : "_")
              .append("`")
              .append(LINE_BREAK);

          if (!StringUtils.isNullOrEmpty(cfgProperty.doc())) {
            configDocBuilder
                .append("> ")
                .append(cfgProperty.doc()).append(LINE_BREAK);
          }

          configDocBuilder
              .append("> ")
              .append("`")
              .append(new Text("Config Class"))
              .append("`")
              .append(": ")
              .append(subType.getName()).append(LINE_BREAK);
          configDocBuilder
              .append("> ")
              .append("`")
              .append(new Text("Config Param"))
              .append("`")
              .append(": ")
              .append(field.getName()).append(LINE_BREAK);

          //configDocBuilder.append(new BoldText("Required Or Optional? ")).append(NEWLINE);
          if (cfgProperty.getSinceVersion().isPresent()) {
            configDocBuilder
                .append("> ")
                .append("`")
                .append(new Text("Since Version"))
                .append("`")
                .append(": ")
                .append(cfgProperty.getSinceVersion().get()).append(LINE_BREAK);
          }
          if (cfgProperty.getDeprecatedVersion().isPresent()) {
            configDocBuilder
                .append("> ")
                .append("`")
                .append(new Text("Deprecated Version"))
                .append("`")
                .append(": ")
                .append(cfgProperty.getDeprecatedVersion().get()).append(LINE_BREAK);
          }
          if (cfgProperty.hasDefaultValue()) {
            configDocBuilder
                .append("> ")
                .append("`")
                .append(new Text("Type of Value"))
                .append("`")
                .append(": ")
                .append(cfgProperty.defaultValue().getClass().getGenericSuperclass()).append(LINE_BREAK);
          }
          //configDocBuilder
          // .append(HYPHEN)
          // .append(new BoldText("Valid Values: ")).append(NEWLINE);
          configDocBuilder
              .append(NEWLINE)
              .append(new HorizontalRule(3))
              .append(DOUBLE_NEWLINE);
        } catch (IllegalAccessException e) {
          LOG.error("Error while getting field through reflection ", e);
        }
      }
    }
    try {
      LOG.info("Generating markdown file");
      Files.write(Paths.get("confid_doc.md"), configDocBuilder.toString().getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      LOG.error("Error while writing to markdown file ", e);
    }
  }

  private static void generateHeader(StringBuilder configDocBuilder) {
    /*
      ---
      title: Configurations
      keywords: garbage collection, hudi, jvm, configs, tuning
      permalink: /docs/configurations.html
      summary: This section offers an overview of tools available to operate an ecosystem of Hudi
      toc: true
      last_modified_at: 2019-12-30T15:59:57-04:00
      ---
     */
    LocalDateTime now = LocalDateTime.now();
    configDocBuilder.append(new HorizontalRule()).append(NEWLINE)
        .append("title: ").append("Configurations").append(NEWLINE)
        .append("keywords: garbage collection, hudi, jvm, configs, tuning").append(NEWLINE)
        .append("permalink: /docs/configurations.html").append(NEWLINE)
        .append("summary: " + SUMMARY).append(NEWLINE)
        .append("toc: true").append(NEWLINE)
        .append("last_modified_at: " + DateTimeFormatter.ISO_DATE_TIME.format(now)).append(NEWLINE)
        .append(new HorizontalRule())
        .append(DOUBLE_NEWLINE);
    // Description
    configDocBuilder.append(SUMMARY).append(DOUBLE_NEWLINE);
  }
}
