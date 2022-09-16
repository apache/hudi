import React from "react";
import styles from "@site/src/components/WhyHudi/styles.module.css";

import Title from "@site/src/components/Title";
import WhyHudiCards from "@site/src/components/WhyHudiCards";
import DataDrivenIcon from "@site/src/components/WhyHudi/Icons/data_driven.svg";
import DataStreamIcon from "@site/src/components/WhyHudi/Icons/data_stream.svg";
import DerivedTablesIcon from "@site/src/components/WhyHudi/Icons/data_driven.svg";
import OpenSourceIcon from "@site/src/components/WhyHudi/Icons/open_source.svg";

const WhyHudi = () => {
  const cards = [
    {
      icon: DataDrivenIcon,
      title: "Data Driven",
      subtitle:
        "Lorem Ipsum is simply dummy text of the printing and typesetting industry.",
    },
    {
      icon: DataStreamIcon,
      title: "Open Source",
      subtitle:
        "Lorem Ipsum is simply dummy text of the printing and typesetting industry.",
    },
    {
      icon: DerivedTablesIcon,
      title: "Derived Tables",
      subtitle:
        "Lorem Ipsum is simply dummy text of the printing and typesetting industry.",
    },
    {
      icon: OpenSourceIcon,
      title: "Data Stream",
      subtitle:
        "Lorem Ipsum is simply dummy text of the printing and typesetting industry.",
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
          <div className="text--center text--semibold">
            Lorem Ipsum is simply dummy text of the printing and typesetting
            industry.
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
