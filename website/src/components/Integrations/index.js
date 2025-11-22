import React from "react";
import {
  batchAnalytics,
  biAnalytics,
  cloudStorage,
  databasesData,
  dataStream,
  interactiveAnalytics,
   lakehousePlateformData,
  metastoreData, orchestrationData, streamAnalytics
} from "./data";
import Services from "./Services";
import styles from "./styles.module.css";
import Title from "@site/src/components/Title";

const Integrations = () => {
return(
  <section className={'container'}>
    <div className={styles.integrationWrapper}>
      <Title primaryText="Integrations" />
      <div className={styles.integrationContainer}>
        <div className={styles.serviceGroup}>
          <div className={styles.serviceLeft}>
            <Services name={"Data Stream"} serviceData={dataStream}/>
            <Services name={"Databases"} serviceData={databasesData}/>
          </div>
          <div className={styles.serviceLeft}>
            <Services name={"Cloud Storage"} serviceData={cloudStorage}/>
          </div>
          </div>
        <div className={styles.serviceGroup}>
          <div className={styles.serviceLeft}>
            <Services name={"Lakehouse Platform"} serviceData={lakehousePlateformData}/>
          </div>
          </div>
        <div className={styles.serviceGroup}>
          <div className={styles.serviceLeft}>
            <Services name={"Metastore"} serviceData={metastoreData}/>
          </div>
          </div>

          <div className={styles.serviceGroup}>
            <div className={styles.serviceLeft}>
              <Services name={"BI Analytics"} serviceData={biAnalytics}/>
              <Services name={"Interactive Analytics"} serviceData={interactiveAnalytics}/>
              <Services name={"Batch Analyitics"} serviceData={batchAnalytics}/>
            </div>
            <div className={styles.serviceLeft}>
              <Services name={"Stream Analytics"} serviceData={streamAnalytics}/>
              <Services name={"Orchestration"} serviceData={orchestrationData}/>
            </div>
          </div>
        </div>
      </div>
  </section>
)
}

export default Integrations
