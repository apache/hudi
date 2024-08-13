import React from 'react';
export default function BlogPostItemContainer({children, className}) {
  return <article className={className}>{children}</article>;
}
