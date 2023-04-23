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
import { BasicColumn, FormSchema } from '/@/components/Table';
import { h } from 'vue';
import { Switch } from 'ant-design-vue';
import { useMessage } from '/@/hooks/web/useMessage';
import { fetTokenStatusToggle } from '/@/api/system/token';
import { getNoTokenUserList } from '/@/api/system/user';
import dayjs from 'dayjs';
import { useI18n } from '/@/hooks/web/useI18n';
const { t } = useI18n();

// status enum
const enum StatusEnum {
  On = '1',
  Off = '0',
}

export const columns: BasicColumn[] = [
  {
    title: t('system.token.table.userName'),
    dataIndex: 'username',
    width: 150,
    sorter: true,
  },
  {
    title: t('system.token.table.token'),
    ellipsis: true,
    width: 250,
    dataIndex: 'token',
  },
  {
    title: t('common.description'),
    dataIndex: 'description',
  },
  {
    title: t('common.createTime'),
    dataIndex: 'createTime',
  },
  {
    title: t('system.token.table.expireTime'),
    dataIndex: 'expireTime',
    sorter: true,
  },
  {
    title: t('system.token.table.status'),
    dataIndex: 'userStatus',
    width: 100,
    customRender: ({ record }) => {
      if (!Reflect.has(record, 'pendingStatus')) {
        record.pendingStatus = false;
      }
      return h(Switch, {
        checked: record.userStatus === StatusEnum.On,
        checkedChildren: 'on',
        unCheckedChildren: 'off',
        loading: record.pendingStatus,
        onChange(checked: boolean) {
          record.pendingStatus = true;
          const newStatus = checked ? StatusEnum.On : StatusEnum.Off;
          const { createMessage } = useMessage();

          fetTokenStatusToggle({ tokenId: record.id })
            .then(() => {
              record.userStatus = newStatus;
              createMessage.success(`success`);
            })
            .finally(() => {
              record.pendingStatus = false;
            });
        },
      });
    },
  },
];

export const searchFormSchema: FormSchema[] = [
  {
    field: 'user',
    label: t('system.token.table.userName'),
    component: 'Input',
    colProps: { span: 8 },
  },
];

export const formSchema: FormSchema[] = [
  {
    field: 'userId',
    label: t('system.token.table.userName'),
    component: 'ApiSelect',
    componentProps: {
      api: getNoTokenUserList,
      resultField: 'records',
      labelField: 'username',
      valueField: 'userId',
    },
    rules: [{ required: true, message: t('system.token.selectUserAlertMessage'), trigger: 'blur' }],
  },
  {
    field: 'description',
    label: t('common.description'),
    component: 'InputTextArea',
  },
  {
    field: 'expireTime',
    label: t('system.token.table.expireTime'),
    component: 'DatePicker',
    defaultValue: dayjs('9999-01-01'),
    componentProps: {
      disabled: true,
    },
  },
];
