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
import org.springframework.shell.plugin.support.DefaultPromptProvider;
import org.springframework.stereotype.Component;

/**
 * This class deals with displaying prompt on CLI based on the state.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class HoodiePrompt extends DefaultPromptProvider {

  @Override
  public String getPrompt() {
    if (HoodieCLI.tableMetadata != null) {
      String tableName = HoodieCLI.tableMetadata.getTableConfig().getTableName();
      switch (HoodieCLI.state) {
        case INIT:
          return "hudi->";
        case TABLE:
          return "hudi:" + tableName + "->";
        case SYNC:
          return "hudi:" + tableName + " <==> " + HoodieCLI.syncTableMetadata.getTableConfig().getTableName() + "->";
        default:
          return "hudi:" + tableName + "->";
      }
    }
    return "hudi->";
  }

  @Override
  public String getProviderName() {
    return "Hoodie provider";
  }

}
