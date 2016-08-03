package com.book1.t04_log_analysis.formatter;

import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * 
 * Formatter implementation that simply returns the logback message.
 * 
 * @author tgoetz
 * 
 */
public class MessageFormatter implements Formatter {

    public String format(ILoggingEvent event) {
        return event.getFormattedMessage();
    }

}
