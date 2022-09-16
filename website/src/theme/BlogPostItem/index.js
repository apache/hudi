/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
import React from 'react';
import clsx from 'clsx';
import {MDXProvider} from '@mdx-js/react';
import {translate} from '@docusaurus/Translate';
import Link from '@docusaurus/Link';
import {useBaseUrlUtils} from '@docusaurus/useBaseUrl';
import {usePluralForm} from '@docusaurus/theme-common';
import MDXComponents from '@theme/MDXComponents';
import EditThisPage from '@theme/EditThisPage';
import styles from './styles.module.css';
import Tag from '@theme/Tag';
import AuthorName from "@site/src/components/AuthorName";

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
                {
                    readingTime,
                },
            ),
        );
    };
}

function BlogPostItem(props) {
    const readingTimePlural = useReadingTimePlural();
    const { withBaseUrl } = useBaseUrlUtils();

    const {
        children,
        frontMatter,
        assets,
        metadata,
        truncated,
        isBlogPostPage = false,
    } = props;

    const {
        date,
        formattedDate,
        permalink,
        tags,
        readingTime,
        title,
        editUrl,
        authors,
    } = metadata;
    const image = assets.image ?? frontMatter.image ?? '/assets/images/hudi-logo-medium.png';
    const tagsExists = tags.length > 0;

    const tagsList = () => {
        return (
            <>
                <ul className={clsx(styles.tags, styles.authorTimeTags, 'padding--none', 'margin-left--sm', {[styles.tagsWrapperPostPage]: isBlogPostPage})}>

                    {tags.map(({label, permalink: tagPermalink}) => (
                        <li key={tagPermalink} className={clsx(styles.tag, {[styles.tagPostPage]: isBlogPostPage})}>
                            <Tag className={clsx(styles.greyLink)} name={label} permalink={tagPermalink}/>
                        </li>
                    ))}
                </ul>
            </>
        );
    }
    const AuthorsList = () => {

        const authorsCount = authors.length;
        if (authorsCount === 0) {
            return (
                <div className={clsx(styles.authorTimeTags, "row 'margin-vert--md'")}>
                    <time dateTime={date} itemProp="datePublished">
                        {formattedDate}
                    </time>
                </div>
            )

        }

        return (
            <>
                {isBlogPostPage ? <div className={clsx(styles.blogPostText, "row")}>
                    <time dateTime={date} itemProp="datePublished">
                        {formattedDate}
                    </time>
                    <AuthorName authors={authors} className={styles.blogPostAuthorsList} />

                </div> : <div
                    className={clsx(styles.authorTimeTags, "row 'margin-vert--md'")}>
                    <time dateTime={date} itemProp="datePublished">
                        {formattedDate} by
                    </time>
                    <AuthorName authors={authors} className={styles.authorsList} />
                </div>}

            </>
        );
    }

    const renderPostHeader = () => {
        const TitleHeading = isBlogPostPage ? 'h1' : 'h2';
        return (
            <header className={styles.postHeader}>
                <div>
                    {!isBlogPostPage && image && (
                        <div className="col blogThumbnail" itemProp="blogThumbnail">
                            <Link itemProp="url" to={permalink}>
                                <img
                                    src={withBaseUrl(image, {
                                        absolute: true,
                                    })}
                                    className="blog-image"
                                />
                            </Link>
                        </div>
                    )}
                    <TitleHeading className={styles.blogPostTitle} itemProp="headline">
                        {isBlogPostPage ? (
                            <TitleHeading className={styles.blogPostPageTitle} itemProp="headline">
                                {title}
                            </TitleHeading>
                        ) : (
                            <Link itemProp="url" to={permalink}>
                                <TitleHeading className={styles.blogPostTitle} itemProp="headline">
                                    {title}
                                </TitleHeading>
                            </Link>

                        )}
                    </TitleHeading>
                    <div className={clsx(styles.blogInfo, "margin-top--sm margin-bottom--sm")}>
                        {AuthorsList()}
                        {isBlogPostPage && readingTime && <div className={clsx(styles.blogPostData, { [styles.blogpostReadingTime]: !isBlogPostPage })}>
                            <>
                                {typeof readingTime !== 'undefined' && (
                                    <>
                                        {readingTimePlural(readingTime)}
                                    </>
                                )}
                            </>
                        </div>
                        }
                    </div>
                </div>
                {!!tags.length && (
                    tagsList()
                )}
            </header>
        );
    };

    return (
        <article
            className={clsx({"blog-list-item": !isBlogPostPage})}
            itemProp="blogPost"
            itemScope
            itemType="http://schema.org/BlogPosting">
            {renderPostHeader()}

            {isBlogPostPage && (
                <div className="markdown" itemProp="articleBody">
                    <MDXProvider components={MDXComponents}>{children}</MDXProvider>
                </div>
            )}

            {(tagsExists || truncated) && isBlogPostPage && editUrl && (
                <footer
                    className={clsx('row docusaurus-mt-lg', {
                        [styles.blogPostDetailsFull]: isBlogPostPage,
                    })}>
                    <div className="col margin-top--sm">
                        <EditThisPage editUrl={editUrl}/>
                    </div>
                </footer>
            )}
        </article>
    );
}

export default BlogPostItem;
