package com.book1.t04;

import java.io.Serializable;

import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.book1.t04.func.MovingAverageFunction;

public class EWMA implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(EWMA.class);
	public static enum Time {
		MILLISENDS(1), SECONDS(1000), MINUTES(SECONDS.getTime() * 60), HOURS(MINUTES.getTime() * 60), DAYS(
				HOURS.getTime() * 24), WEEKS(DAYS.getTime() * 7);

		private long millis;

		public long getTime() {
			return millis;
		}

		private Time(long millis) {
			this.millis = millis;
		}

	}

	/**
	 * 趋近1表示平均值比较平均 趋近0表示接近当前值
	 */
	public static final double ONE_MINUTE_ALPHA = 1 - Math.exp(-5d / 60d / 1d);// 0.07995558537067671
	public static final double FIVE_MINUTE_ALPHA = 1 - Math.exp(-5d / 60d / 5d);// 0.01652854617838251
	public static final double FIFTEEN_MINUTE_ALPHA = 1 - Math.exp(-5d / 60d / 15d);// 0.005540151995103271

	private long window;
	private long alphaWindow;
	private long last;
	private double average;
	private double alpha = -1d;

	private boolean sliding = false;

	public EWMA() {

	}

	public EWMA sliding(double count, Time time) {
		return this.sliding((long) (time.getTime() * count));
	}

	public EWMA sliding(long window) {
		this.sliding = true;
		this.window = window;
		return this;
	}

	public EWMA withAlpha(double alpha) {
		if (!(alpha > 0.0d && alpha <= 1.0d)) {
			throw new IllegalArgumentException("must in (0.0,1.0)");
		}
		this.alpha = alpha;
		return this;
	}

	public EWMA wihAlphaWindow(long alphaWindow) {
		this.alpha = -1;
		this.alphaWindow = alphaWindow;
		return this;
	}

	public EWMA withAlphaWindow(double count, Time time) {
		return this.wihAlphaWindow((long) (time.getTime() * count));
	}

	public void mark() {
		mark(System.currentTimeMillis());
	}

	public synchronized void mark(long time) {
		if (this.sliding) {
			if (time - this.last > this.window) {
				this.last = 0;
			}
		}
		if (this.last == 0) {
			this.average = time;
		}
		if (this.last == 0) {
			this.average = 0;
			this.last = time;
		}
		long diff = time - this.last;
		double alpha = this.alpha != -1.0 ? this.alpha : Math.exp(-1.0 * ((double) diff / this.alphaWindow));
		this.average = (1.0 - alpha) * diff + alpha * this.average;
		this.last = time;
		log.debug("diff:{},alpha:{},average:{},last:{}",diff,alpha,average,last);
	}

	public double getAverage() {
		return average;
	}

	public double getAverageIn(Time time) {
		return average == 0.0 ? average : average / time.getTime();
	}

	public double getAverageRatePer(Time time) {
		return average == 0.0 ? average : time.getTime() / average;
	}

}
