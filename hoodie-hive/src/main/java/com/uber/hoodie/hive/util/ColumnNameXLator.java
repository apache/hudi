/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.hive.util;

import com.google.common.collect.Maps;

import java.util.Iterator;
import java.util.Map;

public class ColumnNameXLator {
    private static Map<String, String> xformMap = Maps.newHashMap();

    public static String translateNestedColumn(String colName) {
        Map.Entry entry;
        for (Iterator i$ = xformMap.entrySet().iterator(); i$.hasNext();
             colName = colName.replaceAll((String) entry.getKey(), (String) entry.getValue())) {
            entry = (Map.Entry) i$.next();
        }

        return colName;
    }

    public static String translateColumn(String colName) {
        return colName;
    }

    public static String translate(String colName, boolean nestedColumn) {
        return !nestedColumn ? translateColumn(colName) : translateNestedColumn(colName);
    }

    static {
        xformMap.put("\\$", "_dollar_");
    }
}
