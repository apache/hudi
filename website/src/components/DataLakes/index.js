import React from "react";
import Title from "@site/src/components/Title";
import clsx from "classnames";
import styles from "./styles.module.css";

const DataLake = () => {
    return (
        <section className="data-lake">
            <div className="container">
                <Title primaryText="What is Hudi"/>
                <div className="sub-title text--center text--semibold margin-bottom--md">
                    Apache Hudi is an open data lakehouse platform, built on a high-performance open table format
                    to bring database functionality to your data lakes.
                    Hudi reimagines slow old-school batch data processing with a
                    powerful new incremental processing framework for low latency minute-level analytics.
                </div>

                {/*desktop view*/}
                <img
                  className={clsx("hudi-lake text-center md:", styles.lakeOverviewImage)}
                  src={require("/assets/images/hudi-lake-overview.png").default}
                  alt="Hudi Data Lake"
                />

                {/* mobile view*/}
                <img
                  className={clsx("hudi-lake text-center", styles.lakeOverviewImageMobile)}
                  src={require("/assets/images/hudi-lake-overview-mobile.png").default}
                  alt="Hudi Data Lake"
                />
            </div>
        </section>
    );
}

export default DataLake
