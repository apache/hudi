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
    <BasicTable @register="registerTable">
      <template #toolbar>
        <a-button type="primary" @click="handleCreate" v-auth="'user:add'">
          <Icon icon="ant-design:plus-outlined" />
          {{ t('common.add') }}
        </a-button>
      </template>
      <template #bodyCell="{ column, record }">
        <template v-if="column.dataIndex === 'action'">
          <TableAction :actions="getUserAction(record)" />
        </template>
      </template>
    </BasicTable>
    <UserDrawer @register="registerDrawer" @success="handleSuccess" />
    <UserModal @register="registerModal" />
  </div>
</template>
<script lang="ts">
  import { computed, defineComponent } from 'vue';

  import { BasicTable, useTable, TableAction, ActionItem } from '/@/components/Table';
  import UserDrawer from './components/UserDrawer.vue';
  import UserModal from './components/UserModal.vue';
  import { useDrawer } from '/@/components/Drawer';
  import { deleteUser, getUserList, resetPassword } from '/@/api/system/user';
  import { columns, searchFormSchema } from './user.data';
  import { FormTypeEnum } from '/@/enums/formEnum';
  import { useMessage } from '/@/hooks/web/useMessage';
  import { useUserStoreWithOut } from '/@/store/modules/user';
  import { useModal } from '/@/components/Modal';
  import { UserListRecord } from '/@/api/system/model/userModel';
  import { useI18n } from '/@/hooks/web/useI18n';
  import Icon from '/@/components/Icon';

  export default defineComponent({
    name: 'User',
    components: { BasicTable, UserModal, UserDrawer, TableAction, Icon },
    setup() {
      const { t } = useI18n();
      const userStore = useUserStoreWithOut();
      const userName = computed(() => {
        return userStore.getUserInfo?.username;
      });
      const [registerDrawer, { openDrawer }] = useDrawer();
      const [registerModal, { openModal }] = useModal();
      const { createMessage, Swal } = useMessage();
      const [registerTable, { reload }] = useTable({
        title: t('system.user.table.title'),
        api: getUserList,
        columns,
        formConfig: {
          // labelWidth: 120,
          baseColProps: { style: { paddingRight: '30px' } },
          schemas: searchFormSchema,
          fieldMapToTime: [['createTime', ['createTimeFrom', 'createTimeTo'], 'YYYY-MM-DD']],
        },
        rowKey: 'userId',
        pagination: true,
        striped: false,
        useSearchForm: true,
        showTableSetting: true,
        bordered: false,
        showIndexColumn: false,
        canResize: false,
        actionColumn: {
          width: 200,
          title: t('component.table.operation'),
          dataIndex: 'action',
        },
      });
      function getUserAction(record: UserListRecord): ActionItem[] {
        return [
          {
            icon: 'clarity:note-edit-line',
            tooltip: t('system.user.table.modify'),
            auth: 'user:update',
            ifShow: () => record.username !== 'admin' || userName.value === 'admin',
            onClick: handleEdit.bind(null, record),
          },
          {
            icon: 'carbon:data-view-alt',
            tooltip: t('common.detail'),
            onClick: handleView.bind(null, record),
          },
          {
            icon: 'bx:reset',
            auth: 'user:reset',
            tooltip: t('system.user.table.reset'),
            ifShow: () => record.username !== 'admin' || userName.value === 'admin',
            popConfirm: {
              title: t('system.user.table.resetTip'),
              confirm: handleReset.bind(null, record),
            },
          },
          {
            icon: 'ant-design:delete-outlined',
            color: 'error',
            auth: 'user:delete',
            ifShow: record.username !== 'admin',
            tooltip: t('system.user.table.delete'),
            popConfirm: {
              title: t('system.user.table.deleteTip'),
              confirm: handleDelete.bind(null, record),
            },
          },
        ];
      }
      // user create
      function handleCreate() {
        openDrawer(true, { formType: FormTypeEnum.Create });
      }
      // edit user
      function handleEdit(record: UserListRecord) {
        openDrawer(true, {
          record,
          formType: FormTypeEnum.Edit,
        });
      }

      // see detail
      function handleView(record: UserListRecord) {
        openModal(true, record);
      }

      // delete current user
      async function handleDelete(record: UserListRecord) {
        const hide = createMessage.loading('deleteing');
        try {
          await deleteUser({ userId: record.userId });
          createMessage.success(t('system.user.table.deleteSuccess'));
          reload();
        } catch (error) {
          console.error('user delete fail:', error);
        } finally {
          hide();
        }
      }

      async function handleReset(record: Recordable) {
        const hide = createMessage.loading('reseting');
        try {
          await resetPassword({ usernames: record.username });
          Swal.fire(t('system.user.table.resetSuccess', [record.username]), '', 'success');
        } catch (error) {
          console.error('user password fail:', error);
        } finally {
          hide();
        }
      }

      // add/edit user success
      function handleSuccess() {
        createMessage.success('success');
        reload();
      }

      return {
        t,
        userName,
        registerTable,
        registerDrawer,
        registerModal,
        handleCreate,
        handleEdit,
        handleDelete,
        handleSuccess,
        handleView,
        handleReset,
        getUserAction,
      };
    },
  });
</script>
