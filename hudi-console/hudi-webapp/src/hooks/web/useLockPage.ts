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
import { computed, onUnmounted, unref, watchEffect } from 'vue';
import { useThrottleFn } from '@vueuse/core';

import { useAppStore } from '/@/store/modules/app';
import { useLockStore } from '/@/store/modules/lock';

import { useUserStore } from '/@/store/modules/user';
import { useRootSetting } from '../setting/useRootSetting';

export function useLockPage() {
  const { getLockTime } = useRootSetting();
  const lockStore = useLockStore();
  const userStore = useUserStore();
  const appStore = useAppStore();

  let timeId: TimeoutHandle;

  function clear(): void {
    window.clearTimeout(timeId);
  }

  function resetCalcLockTimeout(): void {
    // not login
    if (!userStore.getToken) {
      clear();
      return;
    }
    const lockTime = appStore.getProjectConfig.lockTime;
    if (!lockTime || lockTime < 1) {
      clear();
      return;
    }
    clear();

    timeId = setTimeout(() => {
      lockPage();
    }, lockTime * 60 * 1000);
  }

  function lockPage(): void {
    lockStore.setLockInfo({
      isLock: true,
      pwd: undefined,
    });
  }

  watchEffect((onClean) => {
    if (userStore.getToken) {
      resetCalcLockTimeout();
    } else {
      clear();
    }
    onClean(() => {
      clear();
    });
  });

  onUnmounted(() => {
    clear();
  });

  const keyupFn = useThrottleFn(resetCalcLockTimeout, 2000);

  return computed(() => {
    if (unref(getLockTime)) {
      return { onKeyup: keyupFn, onMousemove: keyupFn };
    } else {
      clear();
      return {};
    }
  });
}
