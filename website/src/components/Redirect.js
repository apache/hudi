import React from 'react';
import useIsBrowser from '@docusaurus/useIsBrowser';

export default function Redirect({children, url}) {
  const isBrowser = useIsBrowser();
  if (isBrowser) {
    global.window.location.href = url;
  }
  return (
      <span>
        {children}
        or click <a href={url}>here</a>
      </span>
    );
}