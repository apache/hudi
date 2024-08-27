import React from 'react';
import clsx from 'clsx';
export default function HtmlNavbarItem({
  value,
  className,
  mobile = false,
  isDropdownItem = false,
}) {
  const Comp = isDropdownItem ? 'li' : 'div';
  return (
    <Comp
      className={clsx(
        {
          navbar__item: !mobile && !isDropdownItem,
          'menu__list-item': mobile,
        },
        className,
      )}
      dangerouslySetInnerHTML={{__html: value}}
    />
  );
}
