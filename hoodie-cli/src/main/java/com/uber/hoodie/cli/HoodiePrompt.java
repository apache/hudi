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
import org.springframework.shell.plugin.support.DefaultPromptProvider;
import org.springframework.stereotype.Component;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class HoodiePrompt extends DefaultPromptProvider {

    @Override
    public String getPrompt() {
        switch (HoodieCLI.state) {
            case INIT:
                return "hoodie->";
            case DATASET:
                return "hoodie:" + HoodieCLI.tableMetadata.getTableConfig().getTableName() + "->";
            case SYNC:
                return "hoodie:" + HoodieCLI.tableMetadata.getTableConfig().getTableName() + " <==> "
                    + HoodieCLI.syncTableMetadata.getTableConfig().getTableName() + "->";
        }
        if (HoodieCLI.tableMetadata != null)
            return "hoodie:" + HoodieCLI.tableMetadata.getTableConfig().getTableName() + "->";
        return "hoodie->";
    }

    @Override
    public String getProviderName() {
        return "Hoodie provider";
    }

}
