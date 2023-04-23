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
export default {
  projectStatus: {
    all: '所有(状态)',
    notBuild: '未构建',
    building: '构建中',
    buildSuccess: '构建成功',
    buildFailed: '构建失败',
    needRebuild: '需重新构建',
    successful: '成功',
    failed: '失败',
  },
  searchPlaceholder: '请输入关键词搜索',
  form: {
    projectName: '项目名称',
    projectNamePlaceholder: '请输入项目名称',
    projectType: '项目类型',
    projectTypePlaceholder: '请选择项目类型',
    cvs: 'CVS',
    prvkeyPath: '私钥路径',
    prvkeyPathPlaceholder: '私钥文件路径, 如: ~/.ssh/id_rsa',
    gitCredential: '连接方式',
    gitCredentialPlaceholder: 'Git连接方式, http/ssh',
    cvsPlaceholder: 'CVS类型: git/svn',
    repositoryURL: '仓库地址',
    repositoryURLPlaceholder: '请输入该项目的仓库地址',
    repositoryURLRequired: '仓库地址不能为空',
    httpChecked: '仓库地址必须合法的http(s)地址',
    gitChecked: '仓库地址必须合法的ssh地址',
    userName: '用户名',
    userNamePlaceholder: '请输入(访问)该项目仓库的用户名',
    password: '密码',
    passwordPlaceholder: '请输入(访问)该项目仓库的密码',
    branches: '项目分支',
    branchesPlaceholder: '请选择分支',
    pom: 'POM依赖',
    pomPlaceholder: '默认情况下，在根路径中查找pom.xml，你可以手动指定模块来编译pom.xml',
    buildArgs: '构建(项目)参数',
    buildArgsPlaceholder: '构建参数, 比如: -Pprod',
    description: '描述',
    descriptionPlaceholder: '请输入对该项目的描述信息',
    lastBuild: '最近一次构建',
    buildState: '构建状态',
  },
  operationTips: {
    projectNameIsRequiredMessage: '项目名称必填',
    projectNameIsUniqueMessage: '项目名称已存在，请再次输入',
    projectTypeIsRequiredMessage: '项目类型必选',
    gitCredentialIsRequiredMessage: 'Git连接方式为必填项',
    cvsIsRequiredMessage: '资源来源必选',
    repositoryURLIsRequiredMessage: '(项目)仓库地址必填',
    pomSpecifiesModuleMessage: '指定编译pom.xml的模块 如未指定，则在根路径pom.xml下找到',
    projectIsbuildingMessage: '当前项目正在构件中',
    projectIsbuildFailedMessage: '构建失败',
    deleteProjectSuccessMessage: '删除成功',
    deleteProjectFailedMessage: '删除事变',
    deleteProjectFailedDetailMessage: '请检查该项目是否有应用(模块)',
    notAuthorizedMessage: '未鉴权 ..>﹏<.. <br><br> 用户名和密码是必填的',
    authenticationErrorMessage: '鉴权失败 ..>﹏<.. <br><br> 请检查用户名和密码',
    seeBuildLog: '查看构建日志',
    buildProject: '构建项目',
    buildProjectMessage: '确定构建项目?',
    updateProject: '更新项目',
    deleteProject: '删除项目',
    deleteProjectMessage: '确定删除项目?',
  },
};
