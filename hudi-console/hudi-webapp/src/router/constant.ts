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
export const REDIRECT_NAME = 'Redirect';

export const PARENT_LAYOUT_NAME = 'ParentLayout';

export const PAGE_NOT_FOUND_NAME = 'PageNotFound';

export const EXCEPTION_COMPONENT = () => import('/@/views/base/exception/Exception.vue');

/**
 * @description: default layout
 */
export const LAYOUT = () => import('/@/layouts/default/index.vue');

/**
 * @description: parent-layout
 */
export const getParentLayout = (_name?: string) => {
  return () =>
    new Promise((resolve) => {
      resolve({
        name: PARENT_LAYOUT_NAME,
      });
    });
};
const projectPath = '/flink/project';
const settingPath = '/flink/setting';
const variablePath = '/flink/variable';

const applicationPath = '/flink/app';
export const menuMap = {
  [`${projectPath}/add`]: projectPath,
  [`${projectPath}/edit`]: projectPath,
  [`${applicationPath}/add`]: applicationPath,
  [`${applicationPath}/detail`]: applicationPath,
  [`${applicationPath}/edit_flink`]: applicationPath,
  [`${applicationPath}/edit_streampark`]: applicationPath,
  [`${applicationPath}/edit_streampark`]: applicationPath,
  [`${settingPath}/add_cluster`]: settingPath,
  [`${settingPath}/edit_cluster`]: settingPath,
  [`${variablePath}/depend_apps`]: variablePath,
};
