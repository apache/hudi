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
import { ComputedRef, isRef, nextTick, Ref, ref, unref, watch } from 'vue';
import { onMountedOrActivated } from '/@/hooks/core/onMountedOrActivated';
import { useWindowSizeFn } from '/@/hooks/event/useWindowSizeFn';
import { useLayoutHeight } from '/@/layouts/default/content/useContentViewHeight';
import { getViewportOffset } from '/@/utils/domUtils';
import { isNumber, isString } from '/@/utils/is';

export interface CompensationHeight {
  // Use the layout footer height as a condition for judging the compensation height
  useLayoutFooter: boolean;
  // refs HTMLElement
  elements?: Ref[];
}

type Upward = number | string | null | undefined;

/**
 * Dynamically calculate the content height, according to the anchor DOM lowest coordinate to the screen bottom coordinate, according to the incoming DOM height, padding, margin, and other values
 * The result is the right content height
 *
 * @param flag Reactive identity for turning on compute
 * @param anchorRef Anchor component Ref<ElRef | ComponentRef>
 * @param subtractHeightRefs A list of components to subtract height Ref<ElRef | ComponentRef>
 * @param substractSpaceRefs List of components to subtract margins/paddings Ref<ElRef | ComponentRef>
 * @param offsetHeightRef Calculates the responsive height of the offset, which is directly subtracted when calculating the height
 * @param upwardSpace Recursively up subtracts the level of free space or until class is specified A value of 2 means recurses up twice| an ant-layout value means recurses up until .ant-layout is encountered
 * @returns Responsive height
 */
export function useContentHeight(
  flag: ComputedRef<Boolean>,
  anchorRef: Ref,
  subtractHeightRefs: Ref[],
  substractSpaceRefs: Ref[],
  upwardSpace: Ref<Upward> | ComputedRef<Upward> | Upward = 0,
  offsetHeightRef: Ref<number> = ref(0),
) {
  const contentHeight: Ref<Nullable<number>> = ref(null);
  const { footerHeightRef: layoutFooterHeightRef } = useLayoutHeight();
  let compensationHeight: CompensationHeight = {
    useLayoutFooter: true,
  };

  const setCompensation = (params: CompensationHeight) => {
    compensationHeight = params;
  };

  function redoHeight() {
    nextTick(() => {
      calcContentHeight();
    });
  }

  function calcSubtractSpace(
    element: Element | null | undefined,
    direction: 'all' | 'top' | 'bottom' = 'all',
  ): number {
    function numberPx(px: string) {
      return Number(px.replace(/[^\d]/g, ''));
    }
    let subtractHeight = 0;
    const ZERO_PX = '0px';
    if (element) {
      const cssStyle = getComputedStyle(element);
      const marginTop = numberPx(cssStyle?.marginTop ?? ZERO_PX);
      const marginBottom = numberPx(cssStyle?.marginBottom ?? ZERO_PX);
      const paddingTop = numberPx(cssStyle?.paddingTop ?? ZERO_PX);
      const paddingBottom = numberPx(cssStyle?.paddingBottom ?? ZERO_PX);
      if (direction === 'all') {
        subtractHeight += marginTop;
        subtractHeight += marginBottom;
        subtractHeight += paddingTop;
        subtractHeight += paddingBottom;
      } else if (direction === 'top') {
        subtractHeight += marginTop;
        subtractHeight += paddingTop;
      } else {
        subtractHeight += marginBottom;
        subtractHeight += paddingBottom;
      }
    }
    return subtractHeight;
  }

  function getEl(element: any): Nullable<HTMLDivElement> {
    if (element == null) {
      return null;
    }
    return (element instanceof HTMLDivElement ? element : element.$el) as HTMLDivElement;
  }

  async function calcContentHeight() {
    if (!flag.value) {
      return;
    }
    // Add a delay to get the correct height
    await nextTick();

    const anchorEl = getEl(unref(anchorRef));
    if (!anchorEl) {
      return;
    }
    const { bottomIncludeBody } = getViewportOffset(anchorEl);

    // substract elements height
    let substractHeight = 0;
    subtractHeightRefs.forEach((item) => {
      substractHeight += getEl(unref(item))?.offsetHeight ?? 0;
    });

    // subtract margins / paddings
    let substractSpaceHeight = calcSubtractSpace(anchorEl) ?? 0;
    substractSpaceRefs.forEach((item) => {
      substractSpaceHeight += calcSubtractSpace(getEl(unref(item)));
    });

    // upwardSpace
    let upwardSpaceHeight = 0;
    function upward(element: Element | null, upwardLvlOrClass: number | string | null | undefined) {
      if (element && upwardLvlOrClass) {
        const parent = element.parentElement;
        if (parent) {
          if (isString(upwardLvlOrClass)) {
            if (!parent.classList.contains(upwardLvlOrClass)) {
              upwardSpaceHeight += calcSubtractSpace(parent, 'bottom');
              upward(parent, upwardLvlOrClass);
            } else {
              upwardSpaceHeight += calcSubtractSpace(parent, 'bottom');
            }
          } else if (isNumber(upwardLvlOrClass)) {
            if (upwardLvlOrClass > 0) {
              upwardSpaceHeight += calcSubtractSpace(parent, 'bottom');
              upward(parent, --upwardLvlOrClass);
            }
          }
        }
      }
    }
    if (isRef(upwardSpace)) {
      upward(anchorEl, unref(upwardSpace));
    } else {
      upward(anchorEl, upwardSpace);
    }

    let height =
      bottomIncludeBody -
      unref(layoutFooterHeightRef) -
      unref(offsetHeightRef) -
      substractHeight -
      substractSpaceHeight -
      upwardSpaceHeight;

    // compensation height
    const calcCompensationHeight = () => {
      compensationHeight.elements?.forEach((item) => {
        height += getEl(unref(item))?.offsetHeight ?? 0;
      });
    };
    if (compensationHeight.useLayoutFooter && unref(layoutFooterHeightRef) > 0) {
      calcCompensationHeight();
    } else {
      calcCompensationHeight();
    }

    contentHeight.value = height;
  }

  onMountedOrActivated(() => {
    nextTick(() => {
      calcContentHeight();
    });
  });
  useWindowSizeFn(
    () => {
      calcContentHeight();
    },
    50,
    { immediate: true },
  );
  watch(
    () => [layoutFooterHeightRef.value],
    () => {
      calcContentHeight();
    },
    {
      flush: 'post',
      immediate: true,
    },
  );

  return { redoHeight, setCompensation, contentHeight };
}
