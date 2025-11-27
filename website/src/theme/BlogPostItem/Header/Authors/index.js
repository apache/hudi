import React from 'react';
import clsx from 'clsx';
import {useBlogPost} from '@docusaurus/plugin-content-blog/client';
import BlogAuthor from '@theme/Blog/Components/Author';
import styles from './styles.module.css';
// Component responsible for the authors layout
export default function BlogPostItemHeaderAuthors({className}) {
  const {
    metadata: {authors},
    assets,
  } = useBlogPost();
  const authorsCount = authors.length;
  if (authorsCount === 0) {
    return null;
  }
  const imageOnly = authors.every(({name}) => !name);
  const singleAuthor = authors.length === 1;
  return (
    <div
      className={clsx(
        imageOnly ? styles.imageOnlyAuthorRow : 'row',
        styles.wrapper,
        className,
      )}>
      {authors.map((author, idx) => (
        <div
          className={clsx(
              styles.authorWrapper,
            !imageOnly && (singleAuthor ? 'col col--12' : 'col col--6'),
            imageOnly ? styles.imageOnlyAuthorCol : styles.authorCol,
          )}
          key={idx}>
          {author.name}
        </div>
      ))}
    </div>
  );
}
