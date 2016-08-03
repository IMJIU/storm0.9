package com.book1.t04_log_analysis.formatter;

import ch.qos.logback.classic.spi.ILoggingEvent;

public interface Formatter {
	/**
	 * 
	 * @param event
	 * @return
	 */
    String format(ILoggingEvent event);
}
