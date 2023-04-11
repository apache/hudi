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
import type { ModalFunc, ModalFuncProps } from 'ant-design-vue/lib/modal/Modal';

import { Modal, message as Message, notification } from 'ant-design-vue';
import { InfoCircleFilled, CheckCircleFilled, CloseCircleFilled } from '@ant-design/icons-vue';

import { NotificationArgsProps, ConfigProps } from 'ant-design-vue/lib/notification';
import { useI18n } from './useI18n';
import { isString } from '/@/utils/is';
import Swal from 'sweetalert2';
import 'sweetalert2/dist/sweetalert2.min.css';

export interface NotifyApi {
  info(config: NotificationArgsProps): void;
  success(config: NotificationArgsProps): void;
  error(config: NotificationArgsProps): void;
  warn(config: NotificationArgsProps): void;
  warning(config: NotificationArgsProps): void;
  open(args: NotificationArgsProps): void;
  close(key: String): void;
  config(options: ConfigProps): void;
  destroy(): void;
}

export declare type NotificationPlacement = 'topLeft' | 'topRight' | 'bottomLeft' | 'bottomRight';
export declare type IconType = 'success' | 'info' | 'error' | 'warning';
export interface ModalOptionsEx extends Omit<ModalFuncProps, 'iconType'> {
  iconType?: 'warning' | 'success' | 'error' | 'info';
}
export type ModalOptionsPartial = Partial<ModalOptionsEx> & Pick<ModalOptionsEx, 'content'>;

interface ConfirmOptions {
  info: ModalFunc;
  success: ModalFunc;
  error: ModalFunc;
  warn: ModalFunc;
  warning: ModalFunc;
}

function getIcon(iconType: string) {
  if (iconType === 'warning') {
    return <InfoCircleFilled class="modal-icon-warning" />;
  } else if (iconType === 'success') {
    return <CheckCircleFilled class="modal-icon-success" />;
  } else if (iconType === 'info') {
    return <InfoCircleFilled class="modal-icon-info" />;
  } else {
    return <CloseCircleFilled class="modal-icon-error" />;
  }
}

function renderContent({ content }: Pick<ModalOptionsEx, 'content'>) {
  if (isString(content)) {
    return <div innerHTML={`<div class='pt-20px'>${content as string}</div>`}></div>;
  } else {
    return content;
  }
}

/**
 * @description: Create confirmation box
 */
function createConfirm(options: ModalOptionsEx): ConfirmOptions {
  const iconType = options.iconType || 'warning';
  const showIcon = options.iconType;
  Reflect.deleteProperty(options, 'iconType');
  const opt: ModalFuncProps = {
    centered: true,
    icon: showIcon ? getIcon(iconType) : false,
    ...options,
    content: renderContent(options),
  };
  return Modal.confirm(opt) as unknown as ConfirmOptions;
}

const getBaseOptions = () => {
  const { t } = useI18n();
  return {
    okText: t('common.okText'),
    centered: true,
  };
};

function createModalOptions(options: ModalOptionsPartial, icon: string): ModalOptionsPartial {
  return {
    ...getBaseOptions(),
    ...options,
    content: renderContent(options),
    icon: getIcon(icon),
  };
}

function createSuccessModal(options: ModalOptionsPartial) {
  return Modal.success(createModalOptions(options, 'success'));
}

function createErrorModal(options: ModalOptionsPartial) {
  return Modal.error(createModalOptions(options, 'close'));
}

function createInfoModal(options: ModalOptionsPartial) {
  return Modal.info(createModalOptions(options, 'info'));
}

function createWarningModal(options: ModalOptionsPartial) {
  return Modal.warning(createModalOptions(options, 'warning'));
}

function createErrorSwal(msg: string) {
  return Swal.fire('Failed', msg, 'error');
}

notification.config({
  placement: 'topRight',
  duration: 3,
});

/**
 * @description: message
 */
export function useMessage() {
  return {
    Swal,
    createErrorSwal,
    createMessage: Message,
    notification: notification as NotifyApi,
    createConfirm: createConfirm,
    createSuccessModal,
    createErrorModal,
    createInfoModal,
    createWarningModal,
  };
}
