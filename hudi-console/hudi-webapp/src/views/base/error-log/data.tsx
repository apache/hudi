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
import { Tag } from 'ant-design-vue';
import { BasicColumn } from '/@/components/Table';
import { ErrorTypeEnum } from '/@/enums/exceptionEnum';
import { useI18n } from '/@/hooks/web/useI18n';

const { t } = useI18n();

export function getColumns(): BasicColumn[] {
  return [
    {
      dataIndex: 'type',
      title: t('sys.errorLog.tableColumnType'),
      width: 80,
      customRender: ({ text }) => {
        const color =
          text === ErrorTypeEnum.VUE
            ? 'green'
            : text === ErrorTypeEnum.RESOURCE
            ? 'cyan'
            : text === ErrorTypeEnum.PROMISE
            ? 'blue'
            : ErrorTypeEnum.AJAX
            ? 'red'
            : 'purple';
        return <Tag color={color}>{() => text}</Tag>;
      },
    },
    {
      dataIndex: 'url',
      title: 'URL',
      width: 200,
    },
    {
      dataIndex: 'time',
      title: t('sys.errorLog.tableColumnDate'),
      width: 160,
    },
    {
      dataIndex: 'file',
      title: t('sys.errorLog.tableColumnFile'),
      width: 200,
    },
    {
      dataIndex: 'name',
      title: 'Name',
      width: 200,
    },
    {
      dataIndex: 'message',
      title: t('sys.errorLog.tableColumnMsg'),
      width: 300,
    },
    {
      dataIndex: 'stack',
      title: t('sys.errorLog.tableColumnStackMsg'),
    },
  ];
}

export function getDescSchema(): any {
  return getColumns().map((column) => {
    return {
      field: column.dataIndex!,
      label: column.title,
    };
  });
}
