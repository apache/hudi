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
import { Ref, unref, watchEffect } from 'vue';
import { useTimeoutFn } from '/@/hooks/core/useTimeout';

export interface UseModalDragMoveContext {
  draggable: Ref<boolean>;
  destroyOnClose: Ref<boolean | undefined> | undefined;
  visible: Ref<boolean>;
}

export function useModalDragMove(context: UseModalDragMoveContext) {
  const getStyle = (dom: any, attr: any) => {
    return getComputedStyle(dom)[attr];
  };
  const drag = (wrap: any) => {
    if (!wrap) return;
    wrap.setAttribute('data-drag', unref(context.draggable));
    const dialogHeaderEl = wrap.querySelector('.ant-modal-header');
    const dragDom = wrap.querySelector('.ant-modal');

    if (!dialogHeaderEl || !dragDom || !unref(context.draggable)) return;

    dialogHeaderEl.style.cursor = 'move';

    dialogHeaderEl.onmousedown = (e: any) => {
      if (!e) return;
      // Press the mouse button to calculate the distance of the current element from the viewable area
      const disX = e.clientX;
      const disY = e.clientY;
      const screenWidth = document.body.clientWidth; // body width
      const screenHeight = document.documentElement.clientHeight; // Visible area height (should be body height, can not be obtained in some environments)

      const dragDomWidth = dragDom.offsetWidth; // Dialog box width
      const dragDomheight = dragDom.offsetHeight; // Dialog box height

      const minDragDomLeft = dragDom.offsetLeft;

      const maxDragDomLeft = screenWidth - dragDom.offsetLeft - dragDomWidth;
      const minDragDomTop = dragDom.offsetTop;
      const maxDragDomTop = screenHeight - dragDom.offsetTop - dragDomheight;
      // The obtained value is replaced with px regular matching
      const domLeft = getStyle(dragDom, 'left');
      const domTop = getStyle(dragDom, 'top');
      let styL = +domLeft;
      let styT = +domTop;

      // Note In ie the first value obtained is the component that comes with 50% of the move after the assignment is px
      if (domLeft.includes('%')) {
        styL = +document.body.clientWidth * (+domLeft.replace(/%/g, '') / 100);
        styT = +document.body.clientHeight * (+domTop.replace(/%/g, '') / 100);
      } else {
        styL = +domLeft.replace(/px/g, '');
        styT = +domTop.replace(/px/g, '');
      }

      document.onmousemove = function (e) {
        // With an event delegate, the distance traveled is calculated
        let left = e.clientX - disX;
        let top = e.clientY - disY;

        // Boundary processing
        if (-left > minDragDomLeft) {
          left = -minDragDomLeft;
        } else if (left > maxDragDomLeft) {
          left = maxDragDomLeft;
        }

        if (-top > minDragDomTop) {
          top = -minDragDomTop;
        } else if (top > maxDragDomTop) {
          top = maxDragDomTop;
        }

        // Moves the current element
        dragDom.style.cssText += `;left:${left + styL}px;top:${top + styT}px;`;
      };

      document.onmouseup = () => {
        document.onmousemove = null;
        document.onmouseup = null;
      };
    };
  };

  const handleDrag = () => {
    const dragWraps = document.querySelectorAll('.ant-modal-wrap');
    for (const wrap of Array.from(dragWraps)) {
      if (!wrap) continue;
      const display = getStyle(wrap, 'display');
      const draggable = wrap.getAttribute('data-drag');
      if (display !== 'none') {
        // Drag position
        if (draggable === null || unref(context.destroyOnClose)) {
          drag(wrap);
        }
      }
    }
  };

  watchEffect(() => {
    if (!unref(context.visible) || !unref(context.draggable)) {
      return;
    }
    useTimeoutFn(() => {
      handleDrag();
    }, 30);
  });
}
