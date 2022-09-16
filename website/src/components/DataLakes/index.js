import React from "react";
import Title from "@site/src/components/Title";

const DataLake = () => {
    return (
        <section className="data-lake">
            <div className="container">
                <Title primaryText="Hudi" secondaryText="Data Lakes" id="hudi-data-lakes" />
                <div className="sub-title text--center text--semibold">
                    Hudi is a rich platform to build streaming data lakes with incremental
                    data pipelines <br />
                    on a self-managing database layer, while being optimized for lake
                    engines and regular batch processing.
                </div>

                <img
                    className="hudi-lake text-center"
                    src={require("/assets/images/hudi-lake.png").default}
                    alt="Hudi Data Lake"
                />
            </div>
        </section>
    );
}

export default DataLake
