/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { computed, unref } from 'vue';

import { useAppStore } from '/@/store/modules/app';

import { useRouter } from 'vue-router';

/**
 * @description: Full screen display content
 */
export const useFullContent = () => {
  const appStore = useAppStore();
  const router = useRouter();
  const { currentRoute } = router;

  // Whether to display the content in full screen without displaying the menu
  const getFullContent = computed(() => {
    // Query parameters, the full screen is displayed when the address bar has a full parameter
    const route = unref(currentRoute);
    const query = route.query;
    if (query && Reflect.has(query, '__full__')) {
      return true;
    }
    // Return to the configuration in the configuration file
    return appStore.getProjectConfig.fullContent;
  });

  return { getFullContent };
};
