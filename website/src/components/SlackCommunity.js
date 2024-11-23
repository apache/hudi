import useDocusaurusContext from "@docusaurus/useDocusaurusContext";

const SlackCommunity = ({ title, isItalic }) => {
    const { siteConfig } = useDocusaurusContext();
    const { slackUrl } = siteConfig.customFields;
    return (
        <a href={slackUrl} style={{ fontStyle: isItalic ? "italic" : "normal" }} target="_blank" rel="noopener noreferrer">
            {title}
        </a>
    );
}

export default SlackCommunity;
