import React from "react";
import styles from "./styles.module.css";
import LinkButton from "@site/src/components/UI/LinkButton";
import Calendar from "./Icons/calendar.svg";
import Microphone from "./Icons/microphone.svg";

const Events = () => {
    return (
        <section>
             <div className={styles.banner}>
                 <div className="container">
                     <div className={styles.flexContainer}>
                        <div className={styles.bannercontent}>
                            <h3> Upcoming Event: Bengaluru Apache Hudi Meetup hosted @ Navi Technologies</h3>
                            <p className={styles.flexParagraph}><span className={styles.sideMicrophone}><Microphone/></span>
                             In Person Event | <span className={styles.sideCalendar}><Calendar/></span>
                           May 11</p>
                        </div>
                         <div className={styles.joinButton}>
                             <LinkButton class={styles.registerbutton} type="secondary" to="https://www.linkedin.com/posts/apache-hudi_dataengineering-softwareengineering-activity-7190732754311913475-t19y/">
                                             Join Now
                             </LinkButton>
                         </div>
                     </div>
                 </div>
             </div>
        </section>
    );
}
export default Events;
