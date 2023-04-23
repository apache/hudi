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
import {
  MenuParams,
  RolePageParams,
  RolePageListGetResultModel,
  MenuListModel,
  UserListGetResultModel,
} from './model/systemModel';
import { defHttp } from '/@/utils/http/axios';
import { BasicPageParams } from '/@/api/model/baseModel';

enum Api {
  MenuList = '/menu/list',
  RoleMenu = '/role/menu',
  RolePageList = '/role/list',
  UserList = '/user/list',
}

export const getMenuList = (params?: MenuParams): Promise<MenuListModel> => {
  return defHttp.post({ url: Api.MenuList, params });
};

export const getRoleMenu = (params?: MenuParams) =>
  defHttp.post<Array<string>>({
    url: Api.RoleMenu,
    params,
  });
/**
 * get role list
 * @param {RolePageParams} params
 * @returns {Promise<RolePageListGetResultModel>}
 */
export const getRoleListByPage = (params?: RolePageParams) =>
  defHttp.post<RolePageListGetResultModel>({ url: Api.RolePageList, params });

export const getUserListByPage = (params?: BasicPageParams) =>
  defHttp.post<UserListGetResultModel>({ url: Api.UserList, params });
