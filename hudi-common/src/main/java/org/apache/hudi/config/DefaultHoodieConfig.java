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

package org.apache.hudi.config;

import java.io.Serializable;
import java.util.Properties;

/**
 * Default Way to load Hoodie config through a {@link java.util.Properties}.
 */
public class DefaultHoodieConfig implements Serializable {

  protected final Properties props;

  public DefaultHoodieConfig(Properties props) {
    this.props = props;
  }

  public static void setDefaultOnCondition(Properties props, boolean condition, String propName, String defaultValue) {
    if (condition) {
      props.setProperty(propName, defaultValue);
    }
  }

  public static void setDefaultOnCondition(Properties props, boolean condition, DefaultHoodieConfig config) {
    if (condition) {
      props.putAll(config.getProps());
    }
  }

  public Properties getProps() {
    return props;
  }

}
