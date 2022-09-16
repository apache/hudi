import React from "react";
import styles from "@site/src/components/HomepageFeatures/styles.module.css";
import TransactionsRollbackIcon from "@site/src/components/HomepageFeatures/Icons/transactions_rollback.svg";
import SqlIcon from "@site/src/components/HomepageFeatures/Icons/sql.svg";
import CloudClusteringIcon from "@site/src/components/HomepageFeatures/Icons/cloud_clustering.svg";
import StreamingIngestionIcon from "@site/src/components/HomepageFeatures/Icons/streaming_ingestion.svg";
import BuiltMetadataTrackingIcon from "@site/src/components/HomepageFeatures/Icons/built_metadata_tracking.svg";
import CompatibleSchemaIcon from "@site/src/components/HomepageFeatures/Icons/compatible_schema.svg";
import FeaturesBox from "@site/src/components/HomepageFeatures/FeaturesBox";
import Title from "@site/src/components/Title";

const HomepageFeatures = () => {
  const features = [
    {
      icon: null,
      title: "Upserts, Deletes with fast, pluggable indexing.",
      description:
        "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,",
    },
    {
      icon: null,
      title: "Incremental queries, Record level change streams.",
      description:
        "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,",
    },
    {
      icon: TransactionsRollbackIcon,
      title: "Transactions, Rollbacks, Concurrency Control.",
      description:
        "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,",
    },
    {
      icon: SqlIcon,
      title: "SQL Read/Writes from Spark, Presto, Trino, Hive & more.",
      description:
        "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,",
    },
    {
      icon: CloudClusteringIcon,
      title: "Automatic file sizing, data clustering, compactions, cleaning.\t",
      description:
        "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,",
    },
    {
      icon: StreamingIngestionIcon,
      title: "Streaming ingestion, Built-in CDC sources & tools.",
      description:
        "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,",
    },
    {
      icon: BuiltMetadataTrackingIcon,
      title: "Built-in metadata tracking for scalable storage access.\t",
      description:
        "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,",
    },
    {
      icon: CompatibleSchemaIcon,
      title: "Backwards compatible schema evolution and enforcement.",
      description:
        "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,",
    },
    {
      icon: null,
      title: "Backwards compatible schema evolution and enforcement.",
      description:
        "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,",
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
