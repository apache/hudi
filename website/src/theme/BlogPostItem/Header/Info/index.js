import React from 'react';
import clsx from 'clsx';
import {translate} from '@docusaurus/Translate';
import {usePluralForm} from '@docusaurus/theme-common';
import {useDateTimeFormat} from '@docusaurus/theme-common/internal';
import {useBlogPost} from '@docusaurus/plugin-content-blog/client';
import BlogPostItemHeaderAuthors from '@theme/BlogPostItem/Header/Authors';
import styles from './styles.module.css';
// Very simple pluralization: probably good enough for now
function useReadingTimePlural() {
  const {selectMessage} = usePluralForm();
  return (readingTimeFloat) => {
    const readingTime = Math.ceil(readingTimeFloat);
    return selectMessage(
      readingTime,
      translate(
        {
          id: 'theme.blog.post.readingTime.plurals',
          description:
            'Pluralized label for "{readingTime} min read". Use as much plural forms (separated by "|") as your language support (see https://www.unicode.org/cldr/cldr-aux/charts/34/supplemental/language_plural_rules.html)',
          message: 'One min read|{readingTime} min read',
        },
        {readingTime},
      ),
    );
  };
}
function ReadingTime({readingTime}) {
  const readingTimePlural = useReadingTimePlural();
  return <span className={styles.marker}>{readingTimePlural(readingTime)}</span>;
}
function DateTime({date, formattedDate}) {
  return <time dateTime={date}>{formattedDate}</time>;
}
function Spacer() {
  return <span className={styles.spacer}>{' · '}</span>;
}
export default function BlogPostItemHeaderInfo({className}) {
  const {metadata} = useBlogPost();
  const {date, readingTime} = metadata;
  const dateTimeFormat = useDateTimeFormat({
    day: 'numeric',
    month: 'long',
    year: 'numeric',
    timeZone: 'UTC',
  });
  const formatDate = (blogDate) => dateTimeFormat.format(new Date(blogDate));
  return (
    <div className={clsx(styles.container, 'margin-vert--sm', className)}>
      <BlogPostItemHeaderAuthors />
      <DateTime date={date} formattedDate={formatDate(date)} />
      {typeof readingTime !== 'undefined' && (
        <>
          <ReadingTime readingTime={readingTime} />
        </>
      )}
    </div>
  );
}
