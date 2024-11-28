import React from "react";
import Title from "@site/src/components/Title";

const DataLake = () => {
    return (
        <section className="data-lake">
            <div className="container">
                <Title primaryText="What is" secondaryText="Hudi" />
                <div className="sub-title text--center text--semibold margin-bottom--md">
                    Apache Hudi is an open data lakehouse platform, built on a high-performance open table format
                    to bring database functionality to your data lakes. <br/>
                    Hudi reimagines slow old-school batch data processing with a
                    powerful new incremental processing framework for low latency minute-level analytics.
                </div>

                <img
                    className="hudi-lake text-center"
                    src={require("/assets/images/hudi-lake-overview.png").default}
                    alt="Hudi Data Lake"
                />
            </div>
        </section>
    );
}

export default DataLake
