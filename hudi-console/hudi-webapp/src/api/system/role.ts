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
import { defHttp } from '/@/utils/http/axios';
import { RoleParam } from './model/roleModel';

enum Api {
  AddRole = '/role/post',
  UpdateRole = '/role/update',
  DeleteRole = '/role/delete',
  CHECK_NAME = '/role/check/name',
}

/**
 *
 * @param {RoleParam} data role param
 * @returns {Promise<boolean | undefined>}
 */
export function fetchRoleCreate(data: RoleParam): Promise<boolean | undefined> {
  return defHttp.post({ url: Api.AddRole, data });
}

/**
 * role update
 * @param {RoleParam} data role param
 * @returns {Promise<boolean | undefined>}
 */
export function fetchRoleUpdate(data: RoleParam): Promise<boolean | undefined> {
  return defHttp.put({ url: Api.UpdateRole, data });
}

/**
 * delete role
 * @param {String} roleId role id
 * @returns {Promise<boolean | undefined>}
 */
export function fetchRoleDelete({ roleId }: { roleId: string }): Promise<boolean | undefined> {
  return defHttp.delete({ url: Api.DeleteRole, data: { roleId } });
}
/**
 * Check if the role name exists
 * @returns {Promise<boolean>}
 * @param data
 */
export function fetchCheckName(data: { roleName: string }): Promise<boolean> {
  return defHttp.post({ url: Api.CHECK_NAME, data });
}
