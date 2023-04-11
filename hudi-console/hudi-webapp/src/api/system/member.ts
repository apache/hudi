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
import { BasicTableParams } from '../model/baseModel';
import { defHttp } from '/@/utils/http/axios';
import { Result } from '/#/axios';
// @ts-ignore
import { AxiosResponse } from 'axios';
import { AddMemberParams, MemberListRecord, UpdateMemberParams } from './model/memberModel';
import { UserListItem } from '/@/api/base/model/systemModel';

enum MEMBER_API {
  POST = '/member/post',
  UPDATE = '/member/update',
  LIST = '/member/list',
  CHECK_NAME = '/user/check/name',
  DELETE = '/member/delete',
  USERS = '/member/users',
}

/**
 * get candidate users add to the target team
 * @param params
 * @returns
 */
export function fetchAllUsers(params?: {}): Promise<Array<UserListItem>> {
  return defHttp.post({ url: MEMBER_API.USERS, params });
}

/**
 * get member list
 * @param params
 * @returns
 */
export function fetchMemberList(data: BasicTableParams): Promise<MemberListRecord[]> {
  return defHttp.post({ url: MEMBER_API.LIST, data });
}

/**
 * add member
 * @param {String} userName username
 * @param {Number} roleId role id
 * @returns {Promise<boolean>}
 */
export function fetchAddMember(data: AddMemberParams) {
  return defHttp.post({ url: MEMBER_API.POST, data });
}
/**
 * update member
 * @param {UpdateMemberParams} data
 * @returns {Promise<boolean|undefined>}
 */
export function fetchUpdateMember(data: UpdateMemberParams): Promise<boolean | undefined> {
  return defHttp.put({ url: MEMBER_API.UPDATE, data });
}

/**
 * name check
 * @param {Object} data username
 * @returns {Promise<boolean>}
 */
export function fetchCheckUserName(data: { username: string }): Promise<boolean> {
  return defHttp.post({ url: MEMBER_API.CHECK_NAME, data });
}

/**
 * delete
 * @param {String} data memeber Id
 * @returns {Promise<AxiosResponse<Result>>}
 */
export function fetchMemberDelete(data: { id: string }): Promise<AxiosResponse<Result>> {
  return defHttp.delete({ url: MEMBER_API.DELETE, data }, { isReturnNativeResponse: true });
}
