import React from 'react';

export default function Redirect({children, url}) {

  if (global?.window?.location?.href) {
    global.window.location.href = url;
  }

  return (
      <span>
        {children}
        or click <a href={url}>here</a>
      </span>
    );
}
