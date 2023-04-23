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
import { RuleObject, StoreValue } from 'ant-design-vue/lib/form/interface';
import { fetchCheckName } from '/@/api/system/role';
import { BasicColumn } from '/@/components/Table';
import { FormSchema } from '/@/components/Table';
import { useI18n } from '/@/hooks/web/useI18n';
const { t } = useI18n();
export const columns: BasicColumn[] = [
  {
    title: t('system.role.form.roleName'),
    dataIndex: 'roleName',
  },
  {
    title: t('common.createTime'),
    dataIndex: 'createTime',
    sorter: true,
  },
  {
    title: t('common.modifyTime'),
    dataIndex: 'modifyTime',
    sorter: true,
  },
  {
    title: t('common.description'),
    dataIndex: 'remark',
    ellipsis: true,
  },
];

export const searchFormSchema: FormSchema[] = [
  {
    field: 'roleName',
    label: t('system.role.form.roleName'),
    component: 'Input',
    colProps: { span: 8 },
  },
  {
    field: 'createTime',
    label: t('common.createTime'),
    component: 'RangePicker',
    colProps: { span: 8 },
  },
];
export async function handleRoleCheck(_rule: RuleObject, value: StoreValue) {
  if (value) {
    if (value.length > 255) {
      return Promise.reject(t('system.role.form.roleNameLen'));
    } else {
      const res = await fetchCheckName({
        roleName: value,
      });
      if (res) {
        return Promise.resolve();
      } else {
        return Promise.reject(t('system.role.form.exist'));
      }
    }
  } else {
    return Promise.reject(t('system.role.form.empty'));
  }
}
export const formSchema: FormSchema[] = [
  {
    field: 'roleId',
    label: 'Role Id',
    component: 'Input',
    show: false,
  },
  {
    field: 'roleName',
    label: t('system.role.form.roleName'),
    required: true,
    component: 'Input',
    rules: [{ required: true, validator: handleRoleCheck, trigger: 'blur' }],
  },
  {
    label: t('common.description'),
    field: 'remark',
    component: 'InputTextArea',
  },
  {
    label: '',
    field: 'menuId',
    slot: 'menu',
    component: 'Select',
    rules: [{ required: true, message: t('system.role.form.menuIdRequired') }],
  },
];
