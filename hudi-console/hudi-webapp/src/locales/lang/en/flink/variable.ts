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
  addVariable: 'Add Variable',
  variableInfoTitle: 'Variable Info',
  modifyVariable: 'Modify Variable',
  deleteVariable: 'Delete Variable',
  deletePopConfirm: 'Are you sure delete this variable ?',
  add: 'Add',
  success: ' successful',
  fail: ' failed',
  table: {
    title: 'Variable List',
    variableCode: 'Variable Code',
    variableCodePlaceholder: 'Please enter the variable code to search',
    variableValue: 'Variable Value',
    variableValuePlaceholder: 'Please enter Variable Value',
    descriptionPlaceholder: 'Please enter description to search',
    depend: 'depend apps',
    createUser: 'Create User',
    createTime: 'Create Time',
    modifyTime: 'Modify Time',
    description: 'Description',
  },
  form: {
    descriptionMessage: 'exceeds maximum length limit of 100 characters',
    len: 'Sorry, variable code length should be no less than 3 and no more than 50 characters.',
    regExp:
      'Sorry, variable code can only contain letters, numbers, middle bars, bottom bars and dots, and the beginning can only be letters, For example, kafka_cluster.brokers-520',
    exists: 'Sorry, the Variable Code already exists',
    empty: 'Variable Code cannot be empty',
    desensitization: 'Desensitization',
    desensitizationDesc:
      'Whether desensitization is required, e.g: desensitization of sensitive data such as passwords, if enable variable value will be displayed as ********',
  },
  depend: {
    title: 'Variable Depend Apps',
    jobName: 'Application Name',
    nickName: 'Owner',
    headerTitle: 'Variable " {0} " used list',
  },
};
