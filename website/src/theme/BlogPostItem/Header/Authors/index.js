import React from 'react';
import clsx from 'clsx';
import {useBlogPost} from '@docusaurus/plugin-content-blog/client';
import Link from '@docusaurus/Link';
import styles from './styles.module.css';

export default function BlogPostItemHeaderAuthors({className}) {
  const {
    metadata: {authors},
  } = useBlogPost();
  if (authors.length === 0) {
    return null;
  }
  return (
    <div className={clsx(styles.authorWrapper, className)}>
      {authors.map((author, idx) => (
        <div key={idx} className={styles.author}>
          {author.imageURL && (
            <img
              src={author.imageURL}
              alt={author.name}
              className={styles.authorImage}
            />
          )}
          {author.url ? (
            <Link href={author.url} className={styles.authorName}>
              {author.name}
            </Link>
          ) : (
            <span className={styles.authorName}>{author.name}</span>
          )}
        </div>
      ))}
    </div>
  );
}
