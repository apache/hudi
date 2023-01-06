import React from "react";
import styles from "@site/src/components/WhyHudi/styles.module.css";

import Title from "@site/src/components/Title";
import WhyHudiCards from "@site/src/components/WhyHudiCards";
import TrustedPlatformIcon from "@site/src/components/WhyHudi/Icons/trusted_platform.svg";
import DataStreamIcon from "@site/src/components/WhyHudi/Icons/data_stream.svg";
import DerivedTablesIcon from "@site/src/components/WhyHudi/Icons/derived_tables.svg";
import OpenSourceIcon from "@site/src/components/WhyHudi/Icons/open_source.svg";

const WhyHudi = () => {
  const cards = [
    {
      icon: TrustedPlatformIcon,
      title: "Trusted Platform",
      subtitle:
        "Battle tested and proven in production in some of the largest data lakes on the planet.",
    },
    {
      icon: OpenSourceIcon,
      title: "Open Source",
      subtitle:
        "Hudi is a thriving & growing community that is built with contributions from people around the globe.",
    },
    {
      icon: DerivedTablesIcon,
      title: "Derived tables",
      subtitle:
        "Seamlessly create and manage SQL tables on your data lake to build multi-stage incremental pipelines.",
    },
    {
      icon: DataStreamIcon,
      title: "Data streams",
      subtitle:
        "Take advantage of built-in CDC sources and tools for streaming ingestion.",
    },
  ];
  const Icons = OpenSourceIcon;
  return (
    <section className="container">
      <div className={styles.wrapper}>
        <div className={styles.title}>
          <div className={styles.whyHudiTitle}>
            <Title primaryText="Why" secondaryText="Hudi" />
          </div>
          <div className={styles.textWrapper}>
            <div className="text--center text--semibold">
              Take advantage of Hudi’s platform with rich services and tools to
              make your data lake actionable for applications like personalization,
              machine learning, customer 360 and more!
            </div>
          </div>
        </div>
        <div className={styles.cardsWrapper}>
          {cards.map((card, i) => (
            <WhyHudiCards
              key={i}
              icon={card.icon}
              title={card.title}
              subtitle={card.subtitle}
            />
          ))}
        </div>
      </div>
    </section>
  );
};

export default WhyHudi;
