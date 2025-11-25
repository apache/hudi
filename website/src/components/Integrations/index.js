import React from "react";
import {
  dataProcessingItems,
  fileFormatItems,
  databaseItems,
  streamingItems,
  datawarehouseItems,
  interactiveAnalyticsItems,
  lakeStorageItems,
  dataCatalogItems, 
  orchestrationItems, 
  cdcItems,
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
            <Services name={"Data Streaming"} serviceData={streamingItems}/>
            <Services name={"Databases"} serviceData={databaseItems}/>
            <Services name={"CDC"} serviceData={cdcItems}/>
            <Services name={"File Formats"} serviceData={fileFormatItems}/>
          </div>
          </div>
        <div className={styles.serviceGroup}>
          <div className={styles.serviceLeft}>
            <Services name={"Lake Storage"} serviceData={lakeStorageItems}/>
          </div>
          </div>
        <div className={styles.serviceGroup}>
          <div className={styles.serviceLeft}>
            <Services name={"Data Catalogs"} serviceData={dataCatalogItems}/>
          </div>
          </div>

          <div className={styles.serviceGroup}>
            <div className={styles.serviceLeft}>
              <Services name={"Data Warehouses"} serviceData={datawarehouseItems}/>
              <Services name={"Interactive Analytics"} serviceData={interactiveAnalyticsItems}/>
            </div>
            <div className={styles.serviceLeft}>
              <Services name={"Data Processing"} serviceData={dataProcessingItems}/>
              <Services name={"Orchestration"} serviceData={orchestrationItems}/>
            </div>
          </div>
        </div>
      </div>
  </section>
)
}

export default Integrations
