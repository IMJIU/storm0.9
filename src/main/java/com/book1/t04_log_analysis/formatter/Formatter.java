package com.book1.t04.formatter;

import ch.qos.logback.classic.spi.ILoggingEvent;

public interface Formatter {
	/**
	 * 
	 * @param event
	 * @return
	 */
    String format(ILoggingEvent event);
}
