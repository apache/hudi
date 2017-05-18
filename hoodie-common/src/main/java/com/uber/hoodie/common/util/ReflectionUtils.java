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

package com.uber.hoodie.common.util;

import com.uber.hoodie.common.model.HoodieRecordPayload;

import com.uber.hoodie.exception.HoodieException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReflectionUtils {
    private static Map<String, Class<?>> clazzCache = new HashMap<>();

    public static <T extends HoodieRecordPayload> T loadPayload(String recordPayloadClass) throws IOException {
        try {
            if(clazzCache.get(recordPayloadClass) == null) {
                Class<?> clazz = Class.<HoodieRecordPayload>forName(recordPayloadClass);
                clazzCache.put(recordPayloadClass, clazz);
            }
            return (T) clazzCache.get(recordPayloadClass).newInstance();
        } catch (ClassNotFoundException e) {
            throw new IOException("Could not load payload class " + recordPayloadClass, e);
        } catch (InstantiationException e) {
            throw new IOException("Could not load payload class " + recordPayloadClass, e);
        } catch (IllegalAccessException e) {
            throw new IOException("Could not load payload class " + recordPayloadClass, e);
        }
    }

    public static <T> T loadClass(String fqcn) {
        try {
            if(clazzCache.get(fqcn) == null) {
                Class<?> clazz = Class.<HoodieRecordPayload>forName(fqcn);
                clazzCache.put(fqcn, clazz);
            }
            return (T) clazzCache.get(fqcn).newInstance();
        } catch (ClassNotFoundException e) {
            throw new HoodieException("Could not load class " + fqcn, e);
        } catch (InstantiationException e) {
            throw new HoodieException("Could not load class " + fqcn, e);
        } catch (IllegalAccessException e) {
            throw new HoodieException("Could not load class " + fqcn, e);
        }
    }

}
