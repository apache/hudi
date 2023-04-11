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
import { Persistent, BasicKeys } from '/@/utils/cache/persistent';
import { CacheTypeEnum } from '/@/enums/cacheEnum';
import projectSetting from '/@/settings/projectSetting';
import { TOKEN_KEY } from '/@/enums/cacheEnum';

const { permissionCacheType } = projectSetting;
const isLocal = permissionCacheType === CacheTypeEnum.LOCAL;

export function getToken() {
  return getAuthCache(TOKEN_KEY);
}

export function getAuthCache<T>(key: BasicKeys) {
  if (isLocal) {
    return Persistent.getLocal(key) as T;
  } else {
    const sessionCacheValue = Persistent.getSession(key) as T;
    const localCacheValue = Persistent.getLocal(key) as T;
    if (!sessionCacheValue && localCacheValue) {
      Persistent.setSession(key, localCacheValue, true);
      return localCacheValue;
    }
    return sessionCacheValue;
  }
}

export function setAuthCache(key: BasicKeys, value) {
  if (isLocal) {
    return Persistent.setLocal(key, value, true);
  } else {
    Persistent.setLocal(key, value, true);
    return Persistent.setSession(key, value, true);
  }
}

export function clearAuthCache(immediate = true) {
  if (isLocal) {
    return Persistent.clearLocal(immediate);
  } else {
    Persistent.clearLocal(immediate);
    return Persistent.clearSession(immediate);
  }
}
