import React from "react";
import styles from "@site/src/components/HomepageFeatures/styles.module.css";
import {
  MutabilitySupport,
  IncrementalProcessing,
  ACIDTransactions,
  HistoricalTimeTravel,
  Interoperable,
  TableServices,
  RichPlatform,
  MultiModalIndexes,
  SchemaEvolution,
} from "@site/src/components/HomepageFeatures/Icons";
import FeaturesBox from "@site/src/components/HomepageFeatures/FeaturesBox";
import Title from "@site/src/components/Title";

const HomepageFeatures = () => {
  const features = [
    {
      icon: MutabilitySupport,
      title: "Mutability support for all data lake workloads",
      description:
        "Quickly update & delete data with Hudi’s fast, pluggable indexing. This includes streaming workloads, with full support for out-of-order data, bursty traffic & data deduplication.",
      link: "/docs/indexing",
    },
    {
      icon: IncrementalProcessing,
      title: "Improved efficiency by incrementally processing new data",
      description:
        "Replace old-school batch pipelines with incremental streaming on your data lake. Experience faster ingestion and lower processing times for analytical workloads.",
      link: "/blog/2020/08/18/hudi-incremental-processing-on-data-lakes",
    },
    {
      icon: ACIDTransactions,
      title: "ACID Transactional guarantees to your data lake",
      description:
        "Bring transactional guarantees to your data lake, with consistent, atomic writes and concurrency controls tailored for longer-running lake transactions.",
      link: "/docs/use_cases/#acid-transactions",
    },
    {
      icon: HistoricalTimeTravel,
      title: "Unlock historical data with time travel",
      description:
        "Query historical data with the ability to roll back to a table version; debug data versions to understand what changed over time; audit data changes by viewing the commit history.",
      link: "/docs/use_cases/#time-travel",
    },
    {
      icon: Interoperable,
      title: "Interoperable multi-cloud ecosystem support",
      description:
        "Extensive ecosystem support with plug-and-play options for popular data sources & query engines. Build future-proof architectures interoperable with your vendor of choice.",
      link: "/docs/cloud",
    },
    {
      icon: TableServices,
      title: "Comprehensive table services for high-performance analytics",
      description:
        "Fully automated table services that continuously schedule & orchestrate clustering, compaction, cleaning, file sizing & indexing to ensure tables are always ready.",
      link: "/blog/2021/07/21/streaming-data-lake-platform/#table-services",
    },
    {
      icon: RichPlatform,
      title: "A rich platform to build your lakehouse faster",
      description:
        "Effortlessly build your lakehouse with built-in tools for auto ingestion from services like Debezium and Kafka and auto catalog sync for easy discoverability & more.",
      link: "/blog/2022/01/14/change-data-capture-with-debezium-and-apache-hudi",
    },
    {
      icon: MultiModalIndexes,
      title: "Query acceleration through multi-modal indexes.",
      description:
        "Experience faster write transactions on huge/wide tables & faster query performance with first-of-its kind multi-modal indexing subsystem.",
      link: "/blog/2022/05/17/Introducing-Multi-Modal-Index-for-the-Lakehouse-in-Apache-Hudi",
    },
    {
      icon: SchemaEvolution,
      title: "Resilient Pipelines with schema evolution & enforcement",
      description:
        "Easily change the current schema of a Hudi table to adapt to the data that is changing over time and ensure pipeline resilience by failing fast and avoiding data corruption.",
      link: "/docs/schema_evolution",
    },
  ];
  return (
    <section className={styles.featuresWrapper}>
      <div className="container">
        <div className={styles.titleWrapper}>
          <Title primaryText="Hudi" secondaryText="Features" />
        </div>
        <div className={styles.wrapperContainer}>
          {features.map((data, i) => (
            <FeaturesBox key={i} data={data} />
          ))}
        </div>
      </div>
    </section>
  );
};
export default HomepageFeatures;
