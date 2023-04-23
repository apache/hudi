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
    all: 'ALL',
    notBuild: 'Not Build',
    building: 'Building',
    buildSuccess: 'Build Success',
    buildFailed: 'Build Failed',
    needRebuild: 'NEED REBUILD',
    successful: 'SUCCESSFUL',
    failed: 'FAILED',
  },
  searchPlaceholder: 'please enter a keyword search',
  form: {
    projectName: 'Project Name',
    projectNamePlaceholder: 'please input Project Name',
    projectType: 'Project Type',
    projectTypePlaceholder: 'please select project type',
    cvs: 'CVS',
    prvkeyPath: 'Private Key Path',
    prvkeyPathPlaceholder: 'private key path, e.g: ~/.ssh/id_rsa',
    gitCredential: 'Connect Type',
    gitCredentialPlaceholder: 'Git connection type, http/ssh',
    cvsPlaceholder: 'CVS',
    repositoryURL: 'Repository URL',
    repositoryURLPlaceholder: 'The Repository URL for this project',
    repositoryURLRequired: 'The repository URL is required',
    httpChecked: 'The repository address must be a valid http(s) address',
    gitChecked: 'The repository address must be a valid ssh address',
    userName: 'UserName',
    userNamePlaceholder: 'UserName for this project',
    password: 'Password',
    passwordPlaceholder: 'Password for this project',
    branches: 'Branches',
    branchesPlaceholder: 'Select a branch',
    pom: 'POM',
    pomPlaceholder:
      'By default,lookup pom.xml in root path,You can manually specify the module to compile pom.xmlh',
    buildArgs: 'Build Argument',
    buildArgsPlaceholder: 'Build Argument, e.g: -Pprod',
    description: 'description',
    descriptionPlaceholder: 'Description for this project',
    lastBuild: 'Last Build',
    buildState: 'Build State',
  },
  operationTips: {
    projectNameIsRequiredMessage: 'Project Name is required',
    projectNameIsUniqueMessage: 'The Project Name is already exists. Please check',
    projectTypeIsRequiredMessage: 'Project Type is required',
    gitCredentialIsRequiredMessage: 'Connect Type is required',
    cvsIsRequiredMessage: 'CVS is required',
    repositoryURLIsRequiredMessage: 'Repository URL is required',
    pomSpecifiesModuleMessage:
      'Specifies the module to compile pom.xml If it is not specified, it is found under the root path pom.xml',
    projectIsbuildingMessage: 'The current project is building',
    projectIsbuildFailedMessage: 'Build Fail',
    deleteProjectSuccessMessage: 'delete successful',
    deleteProjectFailedMessage: 'Delete Fail',
    deleteProjectFailedDetailMessage: 'Please check if any application belongs to this project',
    notAuthorizedMessage: 'not authorized ..>﹏<.. <br><br> userName and password is required',
    authenticationErrorMessage:
      'authentication error ..>﹏<.. <br><br> please check userName and password',
    seeBuildLog: 'See Build log',
    buildProject: 'Build Project',
    buildProjectMessage: 'Are you sure build this project?',
    updateProject: 'Update Project',
    deleteProject: 'Delete Project',
    deleteProjectMessage: 'Are you sure delete this project ?',
  },
};
