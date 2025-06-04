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
                            <h3> Beyond Adoption: Halodoc Lakehouse Transformation with Apache Hudi</h3>
                            <p className= {styles. flexParagraph}><span className={styles. sideMicrophone}><Microphone/></span>
                            Linkedin: Live Event | <span className={styles.sideCalendar}><Calendar/></span>
                            June 10th, 8AM Pacific Time</p>
                        </div>
                         <div className={styles.joinButton}>
                             <LinkButton class={styles.registerbutton} type="secondary" to="https://www.linkedin.com/events/beyondadoption-halodoc-slakehou7333879866762629120/theater/">
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
