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
import { BasicPageParams, BasicFetchResult } from '/@/api/model/baseModel';

export type RoleParams = {
  roleName?: string;
  status?: string;
};

export type RolePageParams = BasicPageParams & RoleParams;

export type MenuParams = {
  menuName?: string;
  status?: string;
  roleId?: number;
  type?: string;
};

export interface UserListItem {
  userId: string;
  username: string;
  nickName: string;
  createTime: string;
  status: number;
}

export interface RoleListItem {
  id: string;
  roleName: string;
  roleValue: string;
  status: number;
  orderNo: string;
  createTime: string;
}

export type UserListGetResultModel = BasicFetchResult<UserListItem>;

export type RolePageListGetResultModel = BasicFetchResult<RoleListItem>;

export interface MenuListModel {
  ids: Array<string>;
  rows: any;
  total: number;
}
