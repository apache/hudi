<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<template>
  <div>
    <BasicTable @register="registerTable" @fetch-success="onFetchSuccess" />
    <MenuDrawer
      :okText="t('common.submitText')"
      @register="registerDrawer"
      @success="handleSuccess"
    />
  </div>
</template>
<script lang="ts">
  import { defineComponent, nextTick } from 'vue';

  import { BasicTable, useTable } from '/@/components/Table';
  import { getMenuList } from '/@/api/base/system';

  import { useDrawer } from '/@/components/Drawer';
  import MenuDrawer from './MenuDrawer.vue';

  import { columns, searchFormSchema } from './menu.data';
  import { useMessage } from '/@/hooks/web/useMessage';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { isArray } from '/@/utils/is';

  export default defineComponent({
    name: 'MenuManagement',
    components: { BasicTable, MenuDrawer },
    setup() {
      const [registerDrawer, { openDrawer }] = useDrawer();
      const { createMessage } = useMessage();
      const { t } = useI18n();
      const [registerTable, { reload, expandAll }] = useTable({
        title: t('system.menu.table.title'),
        api: getMenuList,
        afterFetch(result) {
          try {
            handleMenuName(result);
          } catch (error) {
            console.error(error);
          }
          return result;
        },
        columns,
        formConfig: {
          baseColProps: { style: { paddingRight: '30px' } },
          schemas: searchFormSchema,
          fieldMapToTime: [['createTime', ['createTimeFrom', 'createTimeTo'], 'YYYY-MM-DD']],
        },
        fetchSetting: { listField: 'rows.children' },
        isTreeTable: true,
        pagination: false,
        striped: false,
        useSearchForm: true,
        showTableSetting: true,
        bordered: true,
        showIndexColumn: false,
        canResize: false,
        // actionColumn: {
        //   width: 100,
        //   title: 'Operation',
        //   dataIndex: 'action',
        // },
      });
      function handleMenuName(menus: Recordable[]) {
        if (isArray(menus)) {
          menus.forEach((menu: Recordable) => {
            if (menu.children && menu.children.length > 0) handleMenuName(menu.children);
            if (menu.display && menu.type == '0' && /^\w+\.\w+$/.test(menu.title)) {
              menu.text = t(`menu.${menu.text}`);
            }
          });
        }
      }
      function handleCreate() {
        openDrawer(true, { isUpdate: false });
      }

      function handleEdit(record: Recordable) {
        openDrawer(true, {
          record,
          isUpdate: true,
        });
      }

      function handleSuccess() {
        createMessage.success('success');
        reload();
      }

      function onFetchSuccess() {
        // Demo expands all table items by default
        nextTick(expandAll);
      }

      return {
        t,
        registerTable,
        registerDrawer,
        handleCreate,
        handleEdit,
        handleSuccess,
        onFetchSuccess,
      };
    },
  });
</script>
