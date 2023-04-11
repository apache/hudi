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

package org.apache.hudi.console.system.security.impl.ldap;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.ldap.filter.EqualsFilter;
import org.springframework.stereotype.Component;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;

import java.util.Properties;

@Component
@Configuration
@Slf4j
public class LdapService {

  @Value("${ldap.urls:#{null}}")
  private String ldapUrls;

  @Value("${ldap.base-dn:#{null}}")
  private String ldapBaseDn;

  @Value("${ldap.username:#{null}}")
  private String ldapSecurityPrincipal;

  @Value("${ldap.password:#{null}}")
  private String ldapPrincipalPassword;

  @Value("${ldap.user.identity-attribute:#{null}}")
  private String ldapUserIdentifyingAttribute;

  @Value("${ldap.user.email-attribute:#{null}}")
  private String ldapEmailAttribute;

  @Value("${ldap.user.not-exist-action:CREATE}")
  private String ldapUserNotExistAction;

  /**
   * login by userId and return user email
   *
   * @param userId user identity id
   * @param userPwd user login password
   * @return user email
   */
  public String ldapLogin(String userId, String userPwd) {
    Properties searchEnv = getManagerLdapEnv();
    try {
      LdapContext ctx = new InitialLdapContext(searchEnv, null);
      SearchControls sc = new SearchControls();
      sc.setReturningAttributes(new String[] {ldapEmailAttribute});
      sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
      EqualsFilter filter = new EqualsFilter(ldapUserIdentifyingAttribute, userId);
      NamingEnumeration<SearchResult> results = ctx.search(ldapBaseDn, filter.toString(), sc);
      if (results.hasMore()) {
        SearchResult result = results.next();
        NamingEnumeration attrs = result.getAttributes().getAll();
        while (attrs.hasMore()) {
          searchEnv.put(Context.SECURITY_PRINCIPAL, result.getNameInNamespace());
          searchEnv.put(Context.SECURITY_CREDENTIALS, userPwd);
          try {
            new InitialDirContext(searchEnv);
          } catch (Exception e) {
            log.warn("invalid ldap credentials or ldap search error", e);
            return null;
          }
          Attribute attr = (Attribute) attrs.next();
          if (attr.getID().equals(ldapEmailAttribute)) {
            return (String) attr.get();
          }
        }
      }
    } catch (NamingException e) {
      log.error("ldap search error", e);
      return null;
    }
    return null;
  }

  /**
   * * get ldap env fot ldap server search
   *
   * @return Properties
   */
  Properties getManagerLdapEnv() {
    Properties env = new Properties();
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
    env.put(Context.SECURITY_AUTHENTICATION, "simple");
    env.put(Context.SECURITY_PRINCIPAL, ldapSecurityPrincipal);
    env.put(Context.SECURITY_CREDENTIALS, ldapPrincipalPassword);
    env.put(Context.PROVIDER_URL, ldapUrls);
    return env;
  }

  public LdapUserNotExistActionType getLdapUserNotExistAction() {
    if (StringUtils.isBlank(ldapUserNotExistAction)) {
      log.info(
          "security.authentication.ldap.user.not.exist.action configuration is empty, the default value 'CREATE'");
      return LdapUserNotExistActionType.CREATE;
    }

    return LdapUserNotExistActionType.valueOf(ldapUserNotExistAction);
  }

  public boolean createIfUserNotExists() {
    return getLdapUserNotExistAction() == LdapUserNotExistActionType.CREATE;
  }
}
