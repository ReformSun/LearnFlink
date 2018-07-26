package com.test.flatMap_1;

import model.SunWordWithCount;
import model.SunWordWithKey;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class SunFlatMapFunction implements FlatMapFunction<String, SunWordWithCount> {
    private String split = " ";

    public SunFlatMapFunction() {
    }

    @Override
    public void flatMap(String s, Collector<SunWordWithCount> collector) throws Exception {
        for(String ss:s.split(split)){
            collector.collect(new SunWordWithCount(ss,1));
        }
    }


}
