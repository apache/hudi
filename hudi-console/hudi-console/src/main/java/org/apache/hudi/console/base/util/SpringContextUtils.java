/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.console.base.util;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class SpringContextUtils implements ApplicationContextAware {

  private static ApplicationContext applicationContext;

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    SpringContextUtils.applicationContext = applicationContext;
  }

  public static Object getBean(String name) {
    return applicationContext.getBean(name);
  }

  public static <T> T getBean(Class<T> clazz) {
    return applicationContext.getBean(clazz);
  }

  public static <T> T getBean(String name, Class<T> requiredType) {
    return applicationContext.getBean(name, requiredType);
  }

  public static boolean containsBean(String name) {
    return applicationContext.containsBean(name);
  }

  public static boolean isSingleton(String name) {
    return applicationContext.isSingleton(name);
  }

  public static Class<?> getType(String name) {
    return applicationContext.getType(name);
  }
}
