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
                            <h3> Modernizing Data Infrastructure at Southwest Airlines using Apache Hudi</h3>
                            <p className= {styles. flexParagraph}><span className={styles. sideMicrophone}><Microphone/></span>
                            Linkedin: Live Event | <span className={styles.sideCalendar}><Calendar/></span>
                            Mar 19th, 9am Pacific Time</p>
                        </div>
                         <div className={styles.joinButton}>
                             <LinkButton class={styles.registerbutton} type="secondary" to="https://www.linkedin.com/events/modernizingdatainfrastructureat7303469421048573952/theater/">
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
