import React from "react";
import styles from "@site/src/components/JoinCommunity/styles.module.css";
import CommunityCard from "@site/src/components/JoinCommunity/CommunityCard";
import GitHubIcon from "@site/src/components/JoinCommunity/Icons/github.svg";
import SlackIcon from "@site/src/components/JoinCommunity/Icons/slack.svg";
import LinkedinIcon from "@site/src/components/JoinCommunity/Icons/linkedin.svg";
import TwitterIcon from "@site/src/components/JoinCommunity/Icons/twitter.svg";
import XIcon from "@site/src/components/JoinCommunity/Icons/x.svg";
import MailIcon from "@site/src/components/JoinCommunity/Icons/mail.svg";
import YoutubeIcon from "@site/src/components/JoinCommunity/Icons/youtube.svg";
import Title from "@site/src/components/Title";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";

const JoinCommunity = () => {
  const { siteConfig } = useDocusaurusContext();
  const { slackUrl } = siteConfig.customFields;

  const communityData = [
    {
      icon: YoutubeIcon,
      title: "Youtube",
      linkText: "Subscribe",
      url: "https://www.youtube.com/channel/UCs7AhE0BWaEPZSChrBR-Muw",
    },
    {
      icon: LinkedinIcon,
      title: "Linkedin",
      linkText: "Join community",
      url: "https://www.linkedin.com/company/apache-hudi/?viewAsMember=true",
    },
    {
      icon: GitHubIcon,
      title: "GitHub",
      linkText: "Join community",
      url: "https://github.com/apache/hudi",
    },
    {
      icon: SlackIcon,
      title: "Slack",
      linkText: "Join community",
      url: slackUrl,
    },
    {
      icon: MailIcon,
      title: "Mailing",
      linkText: "Subscribe",
      url: "mailto:dev-subscribe@hudi.apache.org?Subject=SubscribeToHudi",
    },
    {
      icon: XIcon,
      title: "X",
      linkText: "Join community",
      url: "https://x.com/ApacheHudi",
    },

  ];

  return (
    <div className={styles.joinCommunityWrapper}>
      <div className="container">
        <div className={styles.communityContent}>
          <div className={styles.leftSideWrapper}>
            <Title primaryText="Join our Community" />
            <p className={styles.communityDescription}>
              Get technical help, influence the product roadmap & see what’s new
              with Hudi!
            </p>
          </div>
          <div className={styles.communityCardWrapper}>
            <div className={styles.communityCardChildWrapper}>
              {communityData.map((media, i) => (
                <CommunityCard key={i} media={media} />
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default JoinCommunity;
