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
  addVariable: '添加变量',
  variableInfoTitle: '变量详情',
  modifyVariable: '修改变量',
  deleteVariable: '删除变量',
  deletePopConfirm: '你确定要删除这个变量?',
  add: '添加',
  success: '成功',
  fail: '失败',
  table: {
    title: '变量列表',
    variableCode: '变量Code',
    variableCodePlaceholder: '输入变量Code查询',
    variableValue: '变量值',
    variableValuePlaceholder: '输入变量值',
    descriptionPlaceholder: '输入描述',
    depend: 'application 依赖',
    createUser: '创建者',
    createTime: '创建时间',
    modifyTime: '修改时间',
    description: '描述',
  },
  form: {
    descriptionMessage: '超过 100 个字符的最大长度限制',
    len: '对不起,变量代码长度应不少于3个,不超过50个字符。',
    regExp:
      '抱歉,变量代码只能包含字母、数字、-、_和. ,开头只能是字母，例如 kafka_cluster.brokers-520',
    exists: '抱歉，变量代码已存在',
    empty: '变量代码不能为空',
    desensitization: '数据脱敏',
    desensitizationDesc:
      '是否需要脱敏，例如：对敏感数据（如密码）进行脱敏，如果启用，变量值将显示为********',
  },
  depend: {
    title: 'application列表',
    jobName: 'Application 名称',
    nickName: '所属者',
    headerTitle: '变量 " {0} " 使用列表',
  },
};
