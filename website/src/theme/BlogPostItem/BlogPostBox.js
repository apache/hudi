import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import styles from './blogPostBoxStyles.module.css';
import AuthorName from "@site/src/components/AuthorName";
import { useBaseUrlUtils } from "@docusaurus/core/lib/client/exports/useBaseUrl";
import Tag from "@theme/Tag";
import {useLocation} from '@docusaurus/router';
export default function BlogPostBox({metadata = {}, assets, frontMatter}) {
    const { withBaseUrl } = useBaseUrlUtils();
    const {
        date,
        permalink,
        tags,
        title,
        authors,
    } = metadata;
    const location = useLocation();

    const image = assets.image ?? frontMatter.image ?? '/assets/images/hudi-logo-medium.png';

    const openResourceInNewTab = (e, resource_url) => {
        e.preventDefault()
        if(resource_url) {
            window.open(resource_url, '_blank', 'noopener noreferrer');
        }
    }

    const tagsList = () => {
        return (
            <ul className={clsx(styles.tags, styles.authorTimeTags, 'padding--none', 'margin-left--sm')}>
                {tags.map(({label, permalink: tagPermalink}) => (
                    <li key={tagPermalink} className={clsx(styles.tag)}>
                        <Tag className={clsx(styles.greyLink)} label={label} permalink={tagPermalink}/>
                    </li>
                ))}
            </ul>
        );
    }
    const AuthorsList = () => {
        const dateObj = new Date(date);
        const options = { year: 'numeric', month: 'long', day: 'numeric' };
        const formattedDate = dateObj.toLocaleDateString('en-US', options);

        const authorsCount = authors.length;
        if (authorsCount === 0) {
            return (
                <div className={clsx(styles.authorTimeTags, "row 'margin-vert--md'")}>
                    {formattedDate}
                </div>
            )
        }

        return (
            <div
                className={clsx(styles.authorTimeTags, "row 'margin-vert--md'")}>
                {formattedDate} by
                <AuthorName authors={authors} className={styles.authorsList} />
            </div>
        );
    }

    const renderPostHeader = () => {
        const TitleHeading =  'h2';
        return (
            <header className={styles.postHeader}>
                <div>
                    {image && (
                        <div className="col blogThumbnail" itemProp="blogThumbnail">
                            {
                                location.pathname.startsWith('/blog') ? <Link itemProp="url" to={permalink}>
                                        <img onClick={(e) => openResourceInNewTab(e, permalink)}
                                            src={withBaseUrl(image, {
                                                absolute: true,
                                            })}
                                            className="blog-image"
                                        />
                                    </Link> :
                                    <img onClick={(e) => openResourceInNewTab(e, frontMatter?.navigate)}
                                         src={withBaseUrl(image, {
                                             absolute: true,
                                         })}
                                         className={clsx(styles.videoImage, 'blog-image')}
                                    />
                            }

                        </div>
                    )}
                    <TitleHeading className={styles.blogPostTitle} itemProp="headline">
                        {location.pathname.startsWith('/blog') ?
                                <Link itemProp="url" to={permalink} onClick={(e) => openResourceInNewTab(e, permalink)}>
                                    <TitleHeading className={styles.blogPostTitle} itemProp="headline">
                                        {title}
                                    </TitleHeading>
                                </Link>
                                :
                                <TitleHeading onClick={(e) => openResourceInNewTab(e, frontMatter?.navigate)}
                                              className={styles.blogPostTitle} itemProp="headline">
                                    {title}
                                </TitleHeading>
                        }
                    </TitleHeading>

                    <div className={clsx(styles.blogInfo, "margin-top--sm margin-bottom--sm")}>
                        {AuthorsList()}
                    </div>
                </div>
                {!!tags.length && (
                    tagsList()
                )}
            </header>
        );
    };



    return(
        <article
            className="blog-list-item"
            itemProp="blogPost"
            itemScope
            itemType="http://schema.org/BlogPosting">
            {renderPostHeader()}
        </article>
    )
}
