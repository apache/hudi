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
  <div class="enter-x mb-50px text-light-50">
    <div class="text-center enter-x">
      <img class="logo w-160px mx-auto my-20px" src="/@/assets/images/logo.png" />
    </div>
  </div>
  <Form
    class="p-4 enter-x signin-form"
    :model="formData"
    :rules="getFormRules"
    ref="formRef"
    v-show="getShow"
    @keypress.enter="handleLogin"
    autocomplete="off"
  >
    <FormItem name="account" class="enter-x">
      <Input
        v-model:value="formData.account"
        size="large"
        :placeholder="t('sys.login.userName')"
        class="fix-auto-fill"
      >
        <template #prefix>
          <user-outlined type="user" />
        </template>
      </Input>
    </FormItem>
    <FormItem name="password" class="enter-x">
      <InputPassword
        visibilityToggle
        size="large"
        v-model:value="formData.password"
        :placeholder="t('sys.login.password')"
      >
        <template #prefix>
          <lock-outlined type="user" />
        </template>
      </InputPassword>
    </FormItem>
    <FormItem class="enter-x">
      <Button
        type="primary"
        class="my-10px"
        size="large"
        block
        @click="handleLogin"
        :loading="loading"
      >
        {{ loginText.buttonText }}
      </Button>
    </FormItem>
    <FormItem class="enter-x text-left">
      <Button type="link" @click="changeLoginType"> {{ loginText.linkText }} </Button>
    </FormItem>
  </Form>
</template>
<script lang="ts" setup>
  import { reactive, ref, unref, computed } from 'vue';
  import { UserOutlined, LockOutlined } from '@ant-design/icons-vue';

  import { Form, Input, Button } from 'ant-design-vue';

  import { useI18n } from '/@/hooks/web/useI18n';
  import { useMessage } from '/@/hooks/web/useMessage';

  import { useUserStore } from '/@/store/modules/user';
  import {
    LoginStateEnum,
    useLoginState,
    useFormRules,
    useFormValid,
    LoginTypeEnum,
  } from './useLogin';
  import { useDesign } from '/@/hooks/web/useDesign';
  import { loginApi, loginLdapApi } from '/@/api/system/user';
  import { LoginResultModel } from '/@/api/system/model/userModel';
  import { Result } from '/#/axios';
  const FormItem = Form.Item;
  const InputPassword = Input.Password;

  const { t } = useI18n();
  const { createErrorModal, createMessage } = useMessage();
  const { prefixCls } = useDesign('login');
  const userStore = useUserStore();
  const { getLoginState } = useLoginState();
  const { getFormRules } = useFormRules();
  interface LoginForm {
    account: string;
    password: string;
  }
  const formRef = ref();
  const loading = ref(false);
  const userId = ref('');
  const modelVisible = ref(false);
  const loginType = ref(LoginTypeEnum.LOCAL);
  const formData = reactive<LoginForm>({
    account: '',
    password: '',
  });

  const loginText = computed(() => {
    const localText = t('sys.login.loginButton');
    const ldapText = t('sys.login.ldapTip');
    if (loginType.value === LoginTypeEnum.LOCAL) {
      return { buttonText: localText, linkText: t('sys.login.ldapTip') };
    }
    return { buttonText: ldapText, linkText: t('sys.login.passwordTip') };
  });

  const { validForm } = useFormValid(formRef);

  const getShow = computed(() => unref(getLoginState) === LoginStateEnum.LOGIN);

  async function handleLogin() {
    try {
      const loginFormValue = await validForm();
      if (!loginFormValue) return;
      await handleLoginAction(loginFormValue);
    } catch (error) {
      console.error(error);
    }
  }

  async function handleLoginRequest(loginFormValue: LoginForm): Promise<Result<LoginResultModel>> {
    // local login
    if (loginType.value == LoginTypeEnum.LOCAL) {
      const { data } = await loginApi(
        { password: loginFormValue.password, username: loginFormValue.account },
        'none',
      );
      return data;
    }
    const { data } = await loginLdapApi(
      { password: loginFormValue.password, username: loginFormValue.account },
      'none',
    );
    return data;
  }
  async function handleLoginAction(loginFormValue: LoginForm) {
    try {
      loading.value = true;
      try {
        const { code, data } = await handleLoginRequest(loginFormValue);
        if (code != null) {
          if (code == 0 || code == 1) {
            const message =
              'SignIn failed,' +
              (code === 0 ? ' authentication error' : ' current User is locked.');
            createMessage.error(message);
            return;
          } else if (code == 403) {
            userId.value = data as unknown as string;
            modelVisible.value = true;
            return;
          } else {
            console.log(data);
          }
        }
        userStore.setData(data);
        let successText = t('sys.login.loginSuccessDesc');
        if (data?.user) {
          const { nickName } = data.user;
          if (nickName) successText += `: ${nickName}`;
        }

        const loginSuccess = await userStore.afterLoginAction(true);
        if (loginSuccess) {
          createMessage.success(`${t('sys.login.loginSuccessTitle')} ${successText}`);
        }
      } catch (error: any) {
        return Promise.reject(error);
      }
    } catch (error) {
      createErrorModal({
        title: t('sys.api.errorTip'),
        content: (error as unknown as Error).message || t('sys.api.networkExceptionMsg'),
        getContainer: () => document.body.querySelector(`.${prefixCls}`) || document.body,
      });
    } finally {
      loading.value = false;
    }
  }

  function changeLoginType() {
    if (loginType.value === LoginTypeEnum.LOCAL) {
      loginType.value = LoginTypeEnum.LDAP;
      return;
    }
    loginType.value = LoginTypeEnum.LOCAL;
  }
</script>
