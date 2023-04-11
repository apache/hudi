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
import { useFullscreen } from '@vueuse/core';
import { computed, ref, unref } from 'vue';
import { useI18n } from '../web/useI18n';

export const useFullscreenEvent = (gloabl = false) => {
  const fullscreenRef = ref<HTMLElement>();
  const { t } = useI18n();
  const { toggle, isFullscreen } = useFullscreen(gloabl ? null : fullscreenRef);

  const getTitle = computed(() => {
    return unref(isFullscreen)
      ? t('layout.header.tooltipExitFull')
      : t('layout.header.tooltipEntryFull');
  });

  return {
    fullscreenRef,
    toggle,
    isFullscreen,
    getTitle,
  };
};

export const useFullContent = () => {
  const fullScreenStatus = ref(false);
  const { t } = useI18n();

  const getTitle = computed(() => {
    return unref(fullScreenStatus)
      ? t('layout.header.tooltipExitFull')
      : t('layout.header.tooltipEntryFull');
  });
  function toggle() {
    fullScreenStatus.value = !fullScreenStatus.value;
  }
  const fullContentClass = computed(() => {
    return {
      [`box-content__full`]: fullScreenStatus.value,
    };
  });
  const fullEditorClass = computed(() => {
    return {
      [`h-[calc(100%-20px)]`]: !fullScreenStatus.value,
      [`h-[calc(100%-88px)]`]: fullScreenStatus.value,
    };
  });
  return {
    fullEditorClass,
    fullContentClass,
    fullScreenStatus,
    getTitle,
    toggle,
  };
};
