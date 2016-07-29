package com.book1.t02_trident.operator;

import org.apache.log4j.Logger;

import com.book1.t02_trident.model.DiagnosisEvent;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class DiseaseFilter extends BaseFilter {
    private static final long serialVersionUID = 1L;
//    private static final Logger LOG = LoggerFactory.getLogger(DiseaseFilter.class);
    private static final Logger LOG = Logger.getLogger(DiseaseFilter.class);
    @Override
    public boolean isKeep(TridentTuple tuple) {
        DiagnosisEvent diagnosis = (DiagnosisEvent) tuple.getValue(0);
        Integer code = Integer.parseInt(diagnosis.diagnosisCode);
        if (code.intValue() <= 322) {
            LOG.debug("Emitting disease [" + diagnosis.diagnosisCode + "]");
            return true;
        } else {
            LOG.debug("Filtering disease [" + diagnosis.diagnosisCode + "]");
            return false;
        }
    }
}