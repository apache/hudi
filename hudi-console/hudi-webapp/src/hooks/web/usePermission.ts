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
import type { RouteRecordRaw } from 'vue-router';

import { useAppStore } from '/@/store/modules/app';
import { usePermissionStore } from '/@/store/modules/permission';
import { useUserStore } from '/@/store/modules/user';

// import { useTabs } from './useTabs';

import { router, resetRouter } from '/@/router';

import projectSetting from '/@/settings/projectSetting';
import { AuthUserName, PermissionModeEnum } from '/@/enums/appEnum';
import { RoleEnum } from '/@/enums/roleEnum';

import { isArray } from '/@/utils/is';
import { useMultipleTabStore } from '/@/store/modules/multipleTab';
import { intersection } from 'lodash-es';
import { PageEnum } from '/@/enums/pageEnum';
import { useRedo } from './usePage';
import { menuMap } from '/@/router/constant';

// User permissions related operations
export function usePermission() {
  const userStore = useUserStore();
  const appStore = useAppStore();
  const permissionStore = usePermissionStore();
  // const { closeAll } = useTabs(router);

  /**
   * Change permission mode
   */
  async function togglePermissionMode() {
    appStore.setProjectConfig({
      permissionMode:
        projectSetting.permissionMode === PermissionModeEnum.BACK
          ? PermissionModeEnum.ROUTE_MAPPING
          : PermissionModeEnum.BACK,
    });
    location.reload();
  }

  /**
   * Reset and regain authority resource information
   * @param id
   */
  async function resume(nextPath?: string) {
    const tabStore = useMultipleTabStore();
    tabStore.clearCacheTabs();

    const redo = useRedo(router);

    resetRouter();
    const [routes, hasAuth] = await permissionStore.buildRoutesAction(nextPath);
    routes.forEach((route) => {
      router.addRoute(route as unknown as RouteRecordRaw);
    });
    permissionStore.setLastBuildMenuTime();
    const currentPath = router.currentRoute.value?.path;
    if (Object.keys(menuMap).includes(currentPath)) {
      router.replace(menuMap[currentPath]);
    } else {
      if (!hasAuth) {
        router.push({ path: PageEnum.BASE_HOME });
      } else {
        await redo();
      }
    }

    // closeAll();
  }

  /**
   * Determine whether there is permission
   */
  function hasPermission(value?: RoleEnum | RoleEnum[] | string | string[], def = true): boolean {
    // Visible by default
    if (!value || userStore.getUserInfo?.username === AuthUserName.ADMIN) {
      return def;
    }

    const permMode = projectSetting.permissionMode;

    if ([PermissionModeEnum.ROUTE_MAPPING, PermissionModeEnum.ROLE].includes(permMode)) {
      if (!isArray(value)) {
        return userStore.getRoleList?.includes(value as RoleEnum);
      }
      return (intersection(value, userStore.getRoleList) as RoleEnum[]).length > 0;
    }

    if (PermissionModeEnum.BACK === permMode) {
      const allCodeList = userStore.getPermissions as string[];
      if (!isArray(value)) {
        return allCodeList.includes(value);
      }
      return (intersection(value, allCodeList) as string[]).length > 0;
    }
    return true;
  }

  /**
   * Change roles
   * @param roles
   */
  async function changeRole(roles: RoleEnum | RoleEnum[]): Promise<void> {
    if (projectSetting.permissionMode !== PermissionModeEnum.ROUTE_MAPPING) {
      throw new Error(
        'Please switch PermissionModeEnum to ROUTE_MAPPING mode in the configuration to operate!',
      );
    }

    if (!isArray(roles)) {
      roles = [roles];
    }
    userStore.setRoleList(roles);
    await resume();
  }

  /**
   * refresh menu data
   */
  async function refreshMenu(nextPath?: string) {
    resume(nextPath);
  }

  return { changeRole, hasPermission, togglePermissionMode, refreshMenu };
}
