import React from "react";
import styles from "@site/src/components/JoinCommunity/styles.module.css";
import ArrowRight from "@site/src/components/JoinCommunity/Icons/arrow_right.svg";

const CommunityCard = ({ media }) => {
  const Icon = media.icon;
  const { title, linkText, url } = media;
  return (
    <div
      className={styles.communityCard}
      onClick={() => window.open(url, "_blank")}
    >
      <Icon />
      <div className={styles.mediaTitle}>
        <p className={styles.title}>{title}</p>
        <p className={styles.linkText}>
          {linkText} <ArrowRight />
        </p>
      </div>
    </div>
  );
};
export default CommunityCard;
