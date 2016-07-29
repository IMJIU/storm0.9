package com.book1.t02_trident.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.book1.t02_trident.model.DiagnosisEvent;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class CityAssignment extends BaseFunction {
	private static final long serialVersionUID = 1L;
//	private static final Logger LOG = LoggerFactory.getLogger(CityAssignment.class);
	private static final Logger LOG = Logger.getLogger(CityAssignment.class);
	private static Map<String, double[]> CITIES = new HashMap<String, double[]>();

	{ // Initialize the cities we care about.
		double[] phl = { 39.875365, -75.249524 };
		CITIES.put("PHL", phl);
		double[] nyc = { 40.71448, -74.00598 };
		CITIES.put("NYC", nyc);
		double[] sf = { -31.4250142, -62.0841809 };
		CITIES.put("SF", sf);
		double[] la = { -34.05374, -118.24307 };
		CITIES.put("LA", la);
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		DiagnosisEvent diagnosis = (DiagnosisEvent) tuple.getValue(0);
		double leastDistance = Double.MAX_VALUE;
		String closestCity = "NONE";
		for (Entry<String, double[]> city : CITIES.entrySet()) {
			double R = 6371; // km
			// x = (x1-x2)*cos((x1+x2)/2)
			// y = (y1-y2)
			double x = (city.getValue()[0] - diagnosis.lng) * Math.cos((city.getValue()[0] + diagnosis.lng) / 2);
			double y = (city.getValue()[1] - diagnosis.lat);
			double d = Math.sqrt(x * x + y * y) * R;
			if (d < leastDistance) {
				leastDistance = d;
				closestCity = city.getKey();
			}
		}
		List<Object> values = new ArrayList<Object>();
		values.add(closestCity);
		LOG.debug("Closest city to lat=[" + diagnosis.lat + "], lng=[" + diagnosis.lng + "] == [" + closestCity+ "], d=[" + leastDistance + "]");
		collector.emit(values);
	}

}
