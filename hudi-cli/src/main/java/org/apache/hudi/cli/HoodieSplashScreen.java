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

package org.apache.hudi.cli;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultBannerProvider;
import org.springframework.shell.support.util.OsUtils;
import org.springframework.stereotype.Component;

/**
 * This class is responsible to print the splash screen at the start of the application.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class HoodieSplashScreen extends DefaultBannerProvider {

  static {
    System.out.println("HoodieSplashScreen loaded");
  }

  private static String screen = "===================================================================" + OsUtils.LINE_SEPARATOR
      + "*         ___                          ___                        *" + OsUtils.LINE_SEPARATOR
      + "*        /\\__\\          ___           /\\  \\           ___         *" + OsUtils.LINE_SEPARATOR
      + "*       / /  /         /\\__\\         /  \\  \\         /\\  \\        *" + OsUtils.LINE_SEPARATOR
      + "*      / /__/         / /  /        / /\\ \\  \\        \\ \\  \\       *" + OsUtils.LINE_SEPARATOR
      + "*     /  \\  \\ ___    / /  /        / /  \\ \\__\\       /  \\__\\      *" + OsUtils.LINE_SEPARATOR
      + "*    / /\\ \\  /\\__\\  / /__/  ___   / /__/ \\ |__|     / /\\/__/      *" + OsUtils.LINE_SEPARATOR
      + "*    \\/  \\ \\/ /  /  \\ \\  \\ /\\__\\  \\ \\  \\ / /  /  /\\/ /  /         *" + OsUtils.LINE_SEPARATOR
      + "*         \\  /  /    \\ \\  / /  /   \\ \\  / /  /   \\  /__/          *" + OsUtils.LINE_SEPARATOR
      + "*         / /  /      \\ \\/ /  /     \\ \\/ /  /     \\ \\__\\          *" + OsUtils.LINE_SEPARATOR
      + "*        / /  /        \\  /  /       \\  /  /       \\/__/          *" + OsUtils.LINE_SEPARATOR
      + "*        \\/__/          \\/__/         \\/__/    Apache Hudi CLI    *" + OsUtils.LINE_SEPARATOR
      + "*                                                                 *" + OsUtils.LINE_SEPARATOR
      + "===================================================================" + OsUtils.LINE_SEPARATOR;

  @Override
  public String getBanner() {
    return screen;
  }

  @Override
  public String getVersion() {
    return "1.0";
  }

  @Override
  public String getWelcomeMessage() {
    return "Welcome to Apache Hudi CLI. Please type help if you are looking for help. ";
  }

  @Override
  public String getProviderName() {
    return "Hoodie Banner";
  }
}
