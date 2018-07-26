package com.test.flatMap_1;

import model.SunWordWithCount;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import sun.security.provider.Sun;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SunFunctionStates1 extends RichFlatMapFunction<SunWordWithCount, SunWordWithCount> {

    private transient ListState<SunWordWithCount> sum;

    @Override
    public void flatMap(SunWordWithCount s, Collector<SunWordWithCount> collector) throws Exception {
        collector.collect(new SunWordWithCount(s.word, 1L));

    }
//    @Override
//    public void open(Configuration parameters) throws Exception {
//
//
//        ListStateDescriptor<SunWordWithCount> descriptor = new ListStateDescriptor<SunWordWithCount>("test",TypeInformation.of(new TypeHint<SunWordWithCount>() {}));
//
//        sum = getRuntimeContext().getListState(descriptor);
//    }
}
