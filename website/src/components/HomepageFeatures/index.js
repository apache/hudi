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
      title: "Mutability support for all workload shapes & sizes",
      description:
        "Quickly update & delete data with fast, pluggable indexing. This includes database CDC and high-scale streaming data, with best-in-class support for out-of-order records, bursty traffic & data deduplication.",
      link: "/docs/indexes",
    },
    {
      icon: IncrementalProcessing,
      title: "Unlock 10x efficiency by incrementally processing new data",
      description:
        "Replace old-school batch pipelines with incremental streaming on your data lake. Experience faster ingestion and lower processing times for your data pipelines.",
      link: "/blog/2020/08/18/hudi-incremental-processing-on-data-lakes",
    },
    {
      icon: ACIDTransactions,
      title: "ACID Transactional guarantees for your data lake",
      description:
        "Atomic writes, with relational/streaming data consistency models, snapshot isolation and non-blocking concurrency controls tailored for longer-running lake transactions.",
      link: "/docs/use_cases/#acid-transactions",
    },
    {
      icon: HistoricalTimeTravel,
      title: "Analyze historical data with time travel",
      description:
        "Query historical data with the ability to roll back to a table version; debug data versions to understand what changed over time; audit data changes by viewing the commit history.",
      link: "/docs/use_cases/#time-travel",
    },
    {
      icon: Interoperable,
      title: "Interoperable multi-cloud ecosystem support",
      description:
        "Built on open data formats with extensive ecosystem support across cloud vendor ecosystem, with plug-and-play options for popular data sources & query engines.",
      link: "/docs/cloud",
    },
    {
      icon: TableServices,
      title: "Automatic table services for a high-performance lakehouse",
      description:
        "Fully automated table services that continuously schedule & orchestrate clustering, compaction, cleaning, file sizing & indexing to ensure tables are always optimized.",
      link: "/blog/2021/07/21/streaming-data-lake-platform/#table-services",
    },
    {
      icon: RichPlatform,
      title: "Open Data Lakehouse platform to get you going faster",
      description:
        "Effortlessly build your lakehouse with built-in tools for auto ingestion from services like Debezium and Kafka and auto catalog sync to major cloud engines & more.",
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
          <Title primaryText="Hudi Features"/>
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
