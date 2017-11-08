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

package com.uber.hoodie.hadoop;


import org.junit.Test;
import static org.junit.Assert.*;
import java.lang.annotation.Annotation;

public class AnnotationTest {

    @Test
    public void testAnnotation() {
        assertTrue(HoodieInputFormat.class.isAnnotationPresent(UseFileSplitsFromInputFormat.class));
        Annotation[] annotations = HoodieInputFormat.class.getAnnotations();
        boolean found = false;
        for (Annotation annotation : annotations) {
            if ("UseFileSplitsFromInputFormat".equals(annotation.annotationType().getSimpleName())){
                found = true;
            }
        }
        assertTrue(found);
    }
}
