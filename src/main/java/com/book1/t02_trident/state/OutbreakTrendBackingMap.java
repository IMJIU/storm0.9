package com.book1.t02_trident.state;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import storm.trident.state.map.IBackingMap;

public class OutbreakTrendBackingMap implements IBackingMap<Long> {
//    private static final Logger LOG = LoggerFactory.getLogger(OutbreakTrendBackingMap.class);
    private static final Logger LOG = Logger.getLogger(OutbreakTrendBackingMap.class);
    Map<String, Long> storage = new ConcurrentHashMap<String, Long>();

    @Override
    public List<Long> multiGet(List<List<Object>> keys) {
        List<Long> values = new ArrayList<Long>();
        for (List<Object> key : keys) {
            Long value = storage.get(key.get(0));
            if (value == null) {
                values.add(new Long(0));
            } else {
                values.add(value);
            }
        }
        return values;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<Long> vals) {
        for (int i = 0; i < keys.size(); i++) {
            System.out.println("Persisting [" + keys.get(i).get(0) + "] ==> [" + vals.get(i) + "]");
            storage.put((String) keys.get(i).get(0), vals.get(i));
        }
    }
}
