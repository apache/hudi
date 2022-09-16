import React from "react";
import styles from '@site/src/components/WhyHudiCards/styles.module.css'

const WhyHudiCards = ({ icon, title, subtitle }) => {
  const Icon = icon;
  return (
    <div className={styles.cardWrapper}>
      <div className={styles.iconWrapper}>{Icon && <Icon />}</div>
      <div className={styles.title}>{title}</div>
      <div className={styles.subtitle}>{subtitle}</div>
    </div>
  );
};

export default WhyHudiCards;
