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
/**
 * @description: Login interface parameters
 */
export interface LoginParams {
  username: string;
  password: string;
}

export interface RoleInfo {
  roleName: string;
  value: string;
}

/**
 * @description: Login interface return value
 */
export interface LoginResultModel {
  expire?: string;
  permissions: string[];
  roles: string[];
  token: string;
  user: GetUserInfoModel;
}

/**
 * @description: Get user information return value
 */
export interface GetUserInfoModel {
  roles: RoleInfo[];
  userId: string | number;
  username: string;
  nickName: string;
  avatar: string;
  desc?: string;
}
export interface TeamSetResponse {
  permissions: string[];
  roles: string[];
  user: GetUserInfoModel;
}

// user list api response
export interface UserListRecord {
  userId: string;
  username: string;
  password: string;
  email?: string;
  userType: string;
  status: string;
  createTime: string;
  modifyTime: string;
  lastLoginTime: string;
  sex: string;
  description?: string;
  avatar?: string;
  sortField?: string;
  sortOrder?: string;
  createTimeFrom?: string;
  createTimeTo?: string;
  id?: string;
  salt: string;
  nickName: string;
}
