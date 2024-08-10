import React from 'react';
export default function BlogPostItemContainer({children, className}) {
  return <article className={`${className} test`}>{children}</article>;
}
