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
                            <h3>Apache Hudi Meetup | ASIA (Chinese)</h3>
                            <p>Next-Generation Lakehouse: The Intelligent Future Engine</p>
                            <p className= {styles. flexParagraph}>
                            <span className={styles.sideCalendar}><Calendar/></span>
                            Oct 23 (Thursday) 14:00 - 17:30 (China Standard Time) | 
                            Beijing JD.com Headquarters</p>
                        </div>
                         <div className={styles.joinButton}>
                             <LinkButton class={styles.registerbutton} type="secondary" to="https://www.bagevent.com/event/9098955">
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
