import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

export const useTitleFormatter = (title, shouldShowOnlyTitle)=> {
    const {siteConfig} = useDocusaurusContext();
    const {title: siteTitle, titleDelimiter} = siteConfig;

    if (shouldShowOnlyTitle && title && title.trim().length) {
        return title.trim();
    }

    return title && title.trim().length
        ? `${title.trim()} ${titleDelimiter} ${siteTitle}`
        : siteTitle;
};
