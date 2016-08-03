package com.book1.t04.filter;

import org.apache.log4j.Logger;

import com.book1.t02_trident.model.DiagnosisEvent;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class BooleanFilter extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tuple) {
        return tuple.getBoolean(0);
//        return false;
    }
}