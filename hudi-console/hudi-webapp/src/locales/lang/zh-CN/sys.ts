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
  api: {
    operationFailed: '操作失败',
    errorTip: '错误提示',
    errorMessage: '操作失败,系统异常!',
    timeoutMessage: '登录超时,请重新登录!',
    apiTimeoutMessage: '接口请求超时,请刷新页面重试!',
    apiRequestFailed: '请求出错，请稍候重试',
    networkException: '网络异常',
    networkExceptionMsg: '网络异常，请检查您的网络连接是否正常!',

    errMsg401: '用户没有权限（令牌、用户名、密码错误）!',
    errMsg403: '用户得到授权，但是访问是被禁止的。!',
    errMsg404: '网络请求错误,未找到该资源!',
    errMsg405: '网络请求错误,请求方法未允许!',
    errMsg408: '网络请求超时!',
    errMsg500: '服务器错误,请联系管理员!',
    errMsg501: '网络未实现!',
    errMsg502: '网络错误!',
    errMsg503: '服务不可用，服务器暂时过载或维护!',
    errMsg504: '网络超时!',
    errMsg505: 'http版本不支持该请求!',
    error403: '抱歉，您无法访问。可能是因为你没有权限或登录无效',
    error404: '抱歉，未找到资源',
    error501: '<a href="https://hudi.apache.org/">查看官方文档?</a>',
    error502:
      '<a href="https://github.com/apache/hudi/issues/new/choose">报告问题 ?</a>',
    errorMsg: '系统错误，请联系管理员',
  },
  app: { logoutTip: '温馨提醒', logoutMessage: '是否确认退出系统?', menuLoading: '菜单加载中...' },
  errorLog: {
    tableTitle: '错误日志列表',
    tableColumnType: '类型',
    tableColumnDate: '时间',
    tableColumnFile: '文件',
    tableColumnMsg: '错误信息',
    tableColumnStackMsg: 'stack信息',

    tableActionDesc: '详情',

    modalTitle: '错误详情',

    fireVueError: '点击触发vue错误',
    fireResourceError: '点击触发资源加载错误',
    fireAjaxError: '点击触发ajax错误',

    enableMessage: '只在`/src/settings/projectSetting.ts` 内的useErrorHandle=true时生效.',
  },
  exception: {
    backLogin: '返回登录',
    backHome: '返回首页',
    subTitle403: '抱歉，您无权访问此页面。',
    subTitle404: '抱歉，您访问的页面不存在。',
    subTitle500: '抱歉，服务器报告错误。',
    noDataTitle: '当前页无数据',
    networkErrorTitle: '网络错误',
    networkErrorSubTitle: '抱歉，您的网络连接已断开，请检查您的网络！',
  },
  lock: {
    unlock: '点击解锁',
    alert: '锁屏密码错误',
    backToLogin: '返回登录',
    entry: '进入系统',
    placeholder: '请输入锁屏密码或者用户密码',
  },
  login: {
    backSignIn: '返回',
    signInFormTitle: '登录',
    mobileSignInFormTitle: '手机登录',
    qrSignInFormTitle: '二维码登录',
    signUpFormTitle: '注册',
    forgetFormTitle: '重置密码',

    signInTitle: '流处理极速开发框架',
    signInDesc: '流批一体 & 湖仓一体的云原生平台, 一站式流处理计算平台!',
    policy: '我同意xxx隐私政策',
    scanSign: `扫码后点击"确认"，即可完成登录`,

    loginButton: '登录',
    registerButton: '注册',
    rememberMe: '记住我',
    forgetPassword: '忘记密码?',
    otherSignIn: '其他登录方式',

    ldapTip: '通过LDAP登录',
    passwordTip: '通过密码登录',

    // notify
    loginSuccessTitle: '登录成功',
    loginSuccessDesc: '欢迎回来',

    // placeholder
    accountPlaceholder: '请输入账号',
    passwordPlaceholder: '请输入密码',
    oldPasswordPlaceholder: '请输入当前密码',
    newPasswordPlaceholder: '请输入新密码',
    confirmPasswordPlaceholder: '请输入确认密码',
    smsPlaceholder: '请输入验证码',
    mobilePlaceholder: '请输入手机号码',
    policyPlaceholder: '勾选后才能注册',
    diffPwd: '两次输入密码不一致',

    userName: '账号',
    password: '密码',
    oldPassword: '当前密码',
    newPassword: '新密码',
    confirmPassword: '确认密码',
    email: '邮箱',
    smsCode: '短信验证码',
    mobile: '手机号码',
  },
  permission: {
    noPermission: '没有权限，请联系管理员',
  },
  modifyPassword: {
    title: '修改密码',
    success: '密码已成功更改,您即将退出系统',
    logout: '立即退出',
  },
};
