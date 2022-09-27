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

package org.apache.hudi.cli.commands;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.IOException;

/**
 * CLI command to perform Kerberos authentication.
 */
@ShellComponent
public class KerberosAuthenticationCommand {

  @ShellMethod(key = "kerberos kinit", value = "Perform Kerberos authentication")
  public String performKerberosAuthentication(
          @ShellOption(value = "--krb5conf", help = "Path to krb5.conf", defaultValue = "/etc/krb5.conf") String krb5ConfPath,
          @ShellOption(value = "--principal", help = "Kerberos principal") String principal,
          @ShellOption(value = "--keytab", help = "Path to keytab") String keytabPath) throws IOException {

    System.out.println("Perform Kerberos authentication");
    System.out.println("Parameters:");
    System.out.println("--krb5conf: " + krb5ConfPath);
    System.out.println("--principal: " + principal);
    System.out.println("--keytab: " + keytabPath);

    System.setProperty("java.security.krb5.conf", krb5ConfPath);
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    conf.set("keytab.file", keytabPath);
    conf.set("kerberos.principal", principal);
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(principal, keytabPath);

    System.out.println("Kerberos current user: " + UserGroupInformation.getCurrentUser());
    System.out.println("Kerberos login user: " + UserGroupInformation.getLoginUser());

    return "Kerberos authentication success";
  }

  @ShellMethod(key = "kerberos kdestroy", value = "Destroy Kerberos authentication")
  public String destroyKerberosAuthentication(
      @ShellOption(value = "--krb5conf", help = "Path to krb5.conf", defaultValue = "/etc/krb5.conf") String krb5ConfPath) throws IOException {

    System.out.println("Destroy Kerberos authentication");
    System.out.println("Parameters:");
    System.out.println("--krb5conf: " + krb5ConfPath);

    System.setProperty("java.security.krb5.conf", krb5ConfPath);
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    if (loginUser.hasKerberosCredentials()) {
      loginUser.logoutUserFromKeytab();
      UserGroupInformation.reset();
    } else {
      System.out.println("Currently, no user login with kerberos, do nothing");
    }

    System.out.println("Current user: " + UserGroupInformation.getCurrentUser());
    System.out.println("Login user: " + UserGroupInformation.getLoginUser());

    return "Destroy Kerberos authentication success";
  }
}
