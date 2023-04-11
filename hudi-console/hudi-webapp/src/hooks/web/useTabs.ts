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
import type { RouteLocationNormalized, RouteLocationRaw, Router } from 'vue-router';

import { useRouter } from 'vue-router';
import { unref } from 'vue';

import { useMultipleTabStore } from '/@/store/modules/multipleTab';
import { useAppStore } from '/@/store/modules/app';

enum TableActionEnum {
  REFRESH,
  CLOSE_ALL,
  CLOSE_LEFT,
  CLOSE_RIGHT,
  CLOSE_OTHER,
  CLOSE_CURRENT,
  CLOSE,
}

export function useTabs(_router?: Router) {
  const appStore = useAppStore();

  function canIUseTabs(): boolean {
    const { show } = appStore.getMultiTabsSetting;
    if (!show) {
      throw new Error('The multi-tab page is currently not open, please open it in the settings！');
    }
    return !!show;
  }

  const tabStore = useMultipleTabStore();
  const router = _router || useRouter();

  const { currentRoute } = router;

  function getCurrentTab() {
    const route = unref(currentRoute);
    return tabStore.getTabList.find((item) => item.fullPath === route.fullPath)!;
  }

  async function updateTabTitle(title: string, tab?: RouteLocationNormalized) {
    const canIUse = canIUseTabs;
    if (!canIUse) {
      return;
    }
    const targetTab = tab || getCurrentTab();
    await tabStore.setTabTitle(title, targetTab);
  }

  async function updateTabPath(path: string, tab?: RouteLocationNormalized) {
    const canIUse = canIUseTabs;
    if (!canIUse) {
      return;
    }
    const targetTab = tab || getCurrentTab();
    await tabStore.updateTabPath(path, targetTab);
  }

  async function handleTabAction(
    action: TableActionEnum,
    tab?: RouteLocationNormalized,
    target?: RouteLocationRaw,
  ) {
    const canIUse = canIUseTabs;
    if (!canIUse) {
      return;
    }
    const currentTab = getCurrentTab();
    switch (action) {
      case TableActionEnum.REFRESH:
        await tabStore.refreshPage(router);
        break;

      case TableActionEnum.CLOSE_ALL:
        await tabStore.closeAllTab(router);
        break;

      case TableActionEnum.CLOSE_LEFT:
        await tabStore.closeLeftTabs(currentTab, router);
        break;

      case TableActionEnum.CLOSE_RIGHT:
        await tabStore.closeRightTabs(currentTab, router);
        break;

      case TableActionEnum.CLOSE_OTHER:
        await tabStore.closeOtherTabs(currentTab, router);
        break;

      case TableActionEnum.CLOSE_CURRENT:
      case TableActionEnum.CLOSE:
        await tabStore.closeTab(tab || currentTab, router, target);
        break;
    }
  }

  return {
    refreshPage: () => handleTabAction(TableActionEnum.REFRESH),
    closeAll: () => handleTabAction(TableActionEnum.CLOSE_ALL),
    closeLeft: () => handleTabAction(TableActionEnum.CLOSE_LEFT),
    closeRight: () => handleTabAction(TableActionEnum.CLOSE_RIGHT),
    closeOther: () => handleTabAction(TableActionEnum.CLOSE_OTHER),
    closeCurrent: () => handleTabAction(TableActionEnum.CLOSE_CURRENT),
    close: (tab?: RouteLocationNormalized, target?: RouteLocationRaw) =>
      handleTabAction(TableActionEnum.CLOSE, tab, target),
    setTitle: (title: string, tab?: RouteLocationNormalized) => updateTabTitle(title, tab),
    updatePath: (fullPath: string, tab?: RouteLocationNormalized) => updateTabPath(fullPath, tab),
  };
}
