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
import { BasicTableParams } from '../model/baseModel';
import { TokenCreateParam, TokenListRecord } from './model/tokenModel';

enum Api {
  TokenList = '/token/list',
  ToggleTokenStatus = '/token/toggle',
  AddToken = '/token/create',
  DeleteToken = '/token/delete',
  CHECK = 'token/check',
  CURL = '/token/curl',
}
/**
 * get token list
 * @param {BasicTableParams} data
 * @returns {Promise<TokenListRecord[]>}
 */
export function fetTokenList(data?: BasicTableParams) {
  return defHttp.post<TokenListRecord[]>({ url: Api.TokenList, data });
}
/**
 * add token
 * @param {TokenCreateParam} data
 * @returns {Promise<TokenListRecord>}
 */
export function fetchTokenCreate(data: TokenCreateParam) {
  return defHttp.post<TokenListRecord>({ url: Api.AddToken, data });
}

/**
 * change token status
 * @returns {Promise<boolean>}
 * @param data
 */
export function fetTokenStatusToggle(data: { tokenId: string }) {
  return defHttp.post<boolean>({ url: Api.ToggleTokenStatus, data });
}

/**
 * delete token
 * @returns {Promise<boolean>}
 * @param data
 */
export function fetchTokenDelete(data?: { tokenId: string }) {
  return defHttp.delete<boolean>({ url: Api.DeleteToken, data });
}

/**
 * check token
 * @param data
 * @returns {Promise<number>}
 */
export function fetchCheckToken(data) {
  return defHttp.post<number>({ url: Api.CHECK, data });
}
/**
 * copyCurl
 * @param data
 * @returns {Promise<string>}
 */
export function fetchCopyCurl(data): Promise<string> {
  return defHttp.post<string>({ url: Api.CURL, data });
}
