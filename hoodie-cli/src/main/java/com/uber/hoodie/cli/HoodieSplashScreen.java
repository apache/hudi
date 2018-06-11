/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.cli;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultBannerProvider;
import org.springframework.shell.support.util.OsUtils;
import org.springframework.stereotype.Component;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class HoodieSplashScreen extends DefaultBannerProvider {

  private static String screen = "============================================" + OsUtils.LINE_SEPARATOR
      + "*                                          *" + OsUtils.LINE_SEPARATOR
      + "*     _    _                 _ _           *" + OsUtils.LINE_SEPARATOR
      + "*    | |  | |               | (_)          *" + OsUtils.LINE_SEPARATOR
      + "*    | |__| | ___   ___   __| |_  ___      *" + OsUtils.LINE_SEPARATOR
      + "*    |  __  |/ _ \\ / _ \\ / _` | |/ _ \\     *" + OsUtils.LINE_SEPARATOR
      + "*    | |  | | (_) | (_) | (_| | |  __/     *" + OsUtils.LINE_SEPARATOR
      + "*    |_|  |_|\\___/ \\___/ \\__,_|_|\\___|     *" + OsUtils.LINE_SEPARATOR
      + "*                                          *" + OsUtils.LINE_SEPARATOR
      + "============================================" + OsUtils.LINE_SEPARATOR;

  public String getBanner() {
    return screen;
  }

  public String getVersion() {
    return "1.0";
  }

  public String getWelcomeMessage() {
    return "Welcome to Hoodie CLI. Please type help if you are looking for help. ";
  }

  @Override
  public String getProviderName() {
    return "Hoodie Banner";
  }
}
