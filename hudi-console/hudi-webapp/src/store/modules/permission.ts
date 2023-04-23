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
import type { AppRouteRecordRaw, Menu } from '/@/router/types';

import { defineStore } from 'pinia';
import { store } from '/@/store';
import { useI18n } from '/@/hooks/web/useI18n';
import { useUserStore } from './user';
import { useAppStoreWithOut } from './app';
import { toRaw } from 'vue';
import { transformObjToRoute, flatMultiLevelRoutes } from '/@/router/helper/routeHelper';
import { transformRouteToMenu } from '/@/router/helper/menuHelper';

import projectSetting from '/@/settings/projectSetting';

import { PermissionModeEnum } from '/@/enums/appEnum';

import { asyncRoutes } from '/@/router/routes';
import { ERROR_LOG_ROUTE, PAGE_NOT_FOUND_ROUTE } from '/@/router/routes/basic';

import { filter } from '/@/utils/helper/treeHelper';

import { getMenuRouter } from '/@/api/system/menu';
import { getPermCode } from '/@/api/system/user';

import { useMessage } from '/@/hooks/web/useMessage';
import { PageEnum } from '/@/enums/pageEnum';

interface PermissionState {
  // Permission code list
  permCodeList: string[] | number[];
  // Whether the route has been dynamically added
  isDynamicAddedRoute: boolean;
  // To trigger a menu update
  lastBuildMenuTime: number;
  // Backstage menu list
  backMenuList: Menu[];
  // menu List
  frontMenuList: Menu[];
}

export const usePermissionStore = defineStore({
  id: 'app-permission',
  state: (): PermissionState => ({
    // List of permission codes
    permCodeList: [],
    // Whether the route has been dynamically added
    isDynamicAddedRoute: false,
    // To trigger a menu update
    lastBuildMenuTime: 0,
    // Backstage menu list
    backMenuList: [],
    // menu List
    frontMenuList: [],
  }),
  getters: {
    getPermCodeList(): string[] | number[] {
      return this.permCodeList;
    },
    getBackMenuList(): Menu[] {
      return this.backMenuList;
    },
    getFrontMenuList(): Menu[] {
      return this.frontMenuList;
    },
    getLastBuildMenuTime(): number {
      return this.lastBuildMenuTime;
    },
    getIsDynamicAddedRoute(): boolean {
      return this.isDynamicAddedRoute;
    },
  },
  actions: {
    setPermCodeList(codeList: string[]) {
      this.permCodeList = codeList;
    },

    setBackMenuList(list: Menu[]) {
      this.backMenuList = list;
      list?.length > 0 && this.setLastBuildMenuTime();
    },

    setFrontMenuList(list: Menu[]) {
      this.frontMenuList = list;
    },

    setLastBuildMenuTime() {
      this.lastBuildMenuTime = new Date().getTime();
    },

    setDynamicAddedRoute(added: boolean) {
      this.isDynamicAddedRoute = added;
    },
    resetState(): void {
      this.isDynamicAddedRoute = false;
      this.permCodeList = [];
      this.backMenuList = [];
      this.lastBuildMenuTime = 0;
    },
    async changePermissionCode() {
      const codeList = await getPermCode();
      this.setPermCodeList(codeList);
    },

    // Build routing
    async buildRoutesAction(nextPath = ''): Promise<[AppRouteRecordRaw[], boolean]> {
      const { t } = useI18n();
      const userStore = useUserStore();
      const appStore = useAppStoreWithOut();
      let hasAuth = false;
      let routes: AppRouteRecordRaw[] = [];
      const roleList = toRaw(userStore.getRoleList) || [];
      const { permissionMode = projectSetting.permissionMode } = appStore.getProjectConfig;

      // The route filter is used in the function filter as a callback incoming traversal
      const routeFilter = (route: AppRouteRecordRaw) => {
        const { meta } = route;
        // Pull out the character
        const { roles } = meta || {};
        if (!roles) return true;
        // Make role permission judgments
        return roleList.some((role) => roles.includes(role));
      };

      const routeRemoveIgnoreFilter = (route: AppRouteRecordRaw) => {
        const { meta } = route;
        // If ignoreRoute is true, the route is only used for menu generation and does not appear in the actual routing table
        const { ignoreRoute } = meta || {};
        // arr.filter returns true to indicate that the element passed the test
        return !ignoreRoute;
      };

      /**
       * @description Fix the affix tag in routes according to the homepage path set (fixed homepage)
       * */
      const patchHomeAffix = (routes: AppRouteRecordRaw[]) => {
        if (!routes || routes.length === 0) return;
        let homePath: string = nextPath || userStore.getUserInfo.homePath || PageEnum.BASE_HOME;
        function patcher(routes: AppRouteRecordRaw[], parentPath = '') {
          if (parentPath) parentPath = parentPath + '/';
          routes.forEach((route: AppRouteRecordRaw) => {
            const { path, children, redirect } = route;
            const currentPath = path.startsWith('/') ? path : parentPath + path;
            if (currentPath === homePath) {
              if (redirect) {
                homePath = route.redirect! as string;
              } else {
                route.meta = Object.assign({}, route.meta, { affix: true });
                hasAuth = true;
                throw new Error('end');
              }
            }
            children && children.length > 0 && patcher(children, currentPath);
          });
        }

        try {
          patcher(routes);
        } catch (e) {
          // Processed out of loop
        }
        return hasAuth;
      };
      switch (permissionMode) {
        // Role authorization
        case PermissionModeEnum.ROLE:
          // Filter for non-first-level routes
          routes = filter(asyncRoutes, routeFilter);
          // Level 1 routes are filtered based on role permissions
          routes = routes.filter(routeFilter);
          // Convert multi-level routing to level 2 routing
          routes = flatMultiLevelRoutes(routes);
          break;

        // Route map, which goes into the case by default
        case PermissionModeEnum.ROUTE_MAPPING:
          // Filter for non-first-level routes
          routes = filter(asyncRoutes, routeFilter);
          // Level 1 routes are filtered again based on role permissions
          routes = routes.filter(routeFilter);
          // Convert routes to menus
          const menuList = transformRouteToMenu(routes, true);
          // Removes the route ignoreRoute: true that is not a Level 1 route
          routes = filter(routes, routeRemoveIgnoreFilter);
          // Remove the ignoreRoute: true route first-level route;
          routes = routes.filter(routeRemoveIgnoreFilter);
          // Sort the menu
          menuList.sort((a, b) => {
            return (a.meta?.orderNo || 0) - (b.meta?.orderNo || 0);
          });

          // Set the menu list
          this.setFrontMenuList(menuList);

          // Convert multi-level routing to level 2 routing
          routes = flatMultiLevelRoutes(routes);
          break;

        //  If you are sure that you do not need to do background dynamic permissions, please comment the entire judgment below
        case PermissionModeEnum.BACK:
          const { createErrorModal, createMessage } = useMessage();

          const hideLoading = createMessage.loading({
            content: t('sys.app.menuLoading'),
            duration: 0,
          });

          try {
            // this function may only need to be executed once, and the actual project can be put at the right time by itself
            let routeList: AppRouteRecordRaw[] = [];
            // await this.changePermissionCode();
            routeList = (await getMenuRouter()) as AppRouteRecordRaw[];
            if (routeList.length == 1 && routeList[0]?.children?.length === 0) {
              createErrorModal({
                title: t('sys.api.errorTip'),
                content: t('sys.permission.noPermission'),
              });
              userStore.logout();
              return Promise.reject(new Error('routeList is empty'));
            }
            routeList = (routeList[0].children as AppRouteRecordRaw[]).map((v) => {
              v.redirect = ((v?.children ?? []) as AppRouteRecordRaw[]).find(
                (item) => !item?.meta?.hidden,
              )?.path;
              return v;
            });
            // Dynamically introduce components
            routeList = transformObjToRoute(routeList);

            //  Background routing to menu structure
            const backMenuList = transformRouteToMenu(routeList);
            this.setBackMenuList(backMenuList);

            // remove meta.ignoreRoute item
            routeList = filter(routeList, routeRemoveIgnoreFilter);
            routeList = routeList.filter(routeRemoveIgnoreFilter);
            routeList = flatMultiLevelRoutes(routeList);
            routes = [PAGE_NOT_FOUND_ROUTE, ...routeList];
          } catch (error) {
            console.error(error);
          } finally {
            hideLoading();
          }
          break;
      }
      routes.push(ERROR_LOG_ROUTE);
      patchHomeAffix(routes);
      return [routes, hasAuth];
    },
  },
});

// Need to be used outside the setup
export function usePermissionStoreWithOut() {
  return usePermissionStore(store);
}
