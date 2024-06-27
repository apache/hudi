import React from "react";
import styles from "@site/src/components/JoinCommunity/styles.module.css";
import CommunityCard from "@site/src/components/JoinCommunity/CommunityCard";
import GitHubIcon from "@site/src/components/JoinCommunity/Icons/github.svg";
import SlackIcon from "@site/src/components/JoinCommunity/Icons/slack.svg";
import LinkedinIcon from "@site/src/components/JoinCommunity/Icons/linkedin.svg";
import TwitterIcon from "@site/src/components/JoinCommunity/Icons/twitter.svg";
import MailIcon from "@site/src/components/JoinCommunity/Icons/mail.svg";
import YoutubeIcon from "@site/src/components/JoinCommunity/Icons/youtube.svg";
import Title from "@site/src/components/Title";

const JoinCommunity = () => {
  const communityData = [
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
      url: "https://join.slack.com/t/apache-hudi/shared_invite/zt-2ggm1fub8-_yt4Reu9djwqqVRFC7X49g",
    },
    {
      icon: LinkedinIcon,
      title: "Linkedin",
      linkText: "Join community",
      url: "https://www.linkedin.com/company/apache-hudi/?viewAsMember=true",
    },
    {
      icon: TwitterIcon,
      title: "Twitter",
      linkText: "Join community",
      url: "https://twitter.com/ApacheHudi",
    },
    {
      icon: YoutubeIcon,
      title: "Youtube",
      linkText: "Subscribe",
      url: "https://www.youtube.com/channel/UCs7AhE0BWaEPZSChrBR-Muw",
    },
    {
      icon: MailIcon,
      title: "Mailing",
      linkText: "Subscribe",
      url: "mailto:dev-subscribe@hudi.apache.org?Subject=SubscribeToHudi",
    },
  ];

  const firstRow = communityData.slice(0,3);
  const secondRow = communityData.slice(3,6);

  return (
    <div className={styles.joinCommunityWrapper}>
      <div className="container">
        <div className={styles.communityContent}>
          <div className={styles.leftSideWrapper}>
            <Title primaryText="Join our" secondaryText="Community" />
            <p className={styles.communityDescription}>
              Get technical help, influence the product roadmap & see what’s new with Hudi!
            </p>
          </div>
          <div className={styles.communityCardWrapper}>
            <div className={styles.communityCardChildWrapper}>
              {firstRow.map((media, i) => <CommunityCard key={i} media={media} />)}
            </div>
            <div className={styles.communityCardChildWrapper}>
              {secondRow.map((media, i) => <CommunityCard key={i} media={media} />)}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default JoinCommunity;
