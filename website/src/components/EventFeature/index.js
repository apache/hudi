import React from "react";
import styles from "./styles.module.css";
import LinkButton from "@site/src/components/UI/LinkButton";
import Calendar from "./Icons/calendar.svg";
import Microphone from "./Icons/microphone.svg";

const Events = () => {
    return (
        <section className="featurebanner">
             <div className={styles.banner}>
                <div className="container">
                    <div className={styles.bannercontent}>
                        <h3> Upcoming Event: Build and Secure AWS Hudi Lakes with AWS Glue & AWS Lake Formations</h3>
                        <p><span className={styles.sideMicrophone}><Microphone/></span> 
                         LinkedIn Live Event | <span className={styles.sideCalendar}><Calendar/></span> 
                       January 25</p> 
                    </div>
                    </div>
                    <LinkButton class={styles.registerbutton} type="secondary" to="https://www.linkedin.com/company/apache-hudi/events/?viewAsMember=true">
                             Join Now
                        </LinkButton>
                    </div>
                    </section>
    );
}
export default Events;
