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
import { ref, computed, unref } from 'vue';
import { createPageContext } from '/@/hooks/component/usePageContext';
import { useWindowSizeFn } from '/@/hooks/event/useWindowSizeFn';

const headerHeightRef = ref(0);
const footerHeightRef = ref(0);

export function useLayoutHeight() {
  function setHeaderHeight(val) {
    headerHeightRef.value = val;
  }
  function setFooterHeight(val) {
    footerHeightRef.value = val;
  }
  return { headerHeightRef, footerHeightRef, setHeaderHeight, setFooterHeight };
}

export function useContentViewHeight() {
  const contentHeight = ref(window.innerHeight);
  const pageHeight = ref(window.innerHeight);
  const getViewHeight = computed(() => {
    return unref(contentHeight) - unref(headerHeightRef) - unref(footerHeightRef) || 0;
  });

  useWindowSizeFn(
    () => {
      contentHeight.value = window.innerHeight;
    },
    100,
    { immediate: true },
  );

  async function setPageHeight(height: number) {
    pageHeight.value = height;
  }

  createPageContext({
    contentHeight: getViewHeight,
    setPageHeight,
    pageHeight,
  });
}
