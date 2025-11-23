import styles from "./styles.module.css";

const Services = ({ name, serviceData }) => {

  return(
    <div className={styles.serviceWrapper}>
      <div className={styles.title}>
        {name}
      </div>
      <div className={styles.serviceListContainer}>
        {
          serviceData?.map((elem, i) => {
            return (
              <div className={styles.serviceList} key={i}>
                <img src={elem.icon} alt={elem.title}/>
                <div className={styles.serviceTitle}>
                  {elem.title}
                </div>
              </div>
            )
          })
        }
      </div>
    </div>
  )
}

export default Services
