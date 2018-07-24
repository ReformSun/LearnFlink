package com.test.flatMap_1;

import model.SunWordWithCount;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
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

public class SunFunctionStates1 extends RichFlatMapFunction<String, SunWordWithCount> {

    private transient ValueState<List<SunWordWithCount>> sum;

    @Override
    public void flatMap(String s, Collector<SunWordWithCount> collector) throws Exception {


        List<SunWordWithCount> currentSunWordWithCounts = sum.value();

        for (String word : s.split(" ")) {
            boolean whetherToAdd = false;
            Iterator<SunWordWithCount> iterator = currentSunWordWithCounts.iterator();
            while (iterator.hasNext()){
                SunWordWithCount sunWordWithCount = iterator.next();
                if (sunWordWithCount.word.equals(word)){
                    whetherToAdd = true;
                    sunWordWithCount.count = sunWordWithCount.count + 1;
                    collector.collect(sunWordWithCount);
                }
            }

            if (!whetherToAdd){
                SunWordWithCount sunWordWithCount = new SunWordWithCount(word,1);
                collector.collect(new SunWordWithCount(word, 1L));
                currentSunWordWithCounts.add(sunWordWithCount);
            }


        }

        sum.update(currentSunWordWithCounts);
    }
    @Override
    public void open(Configuration parameters) throws Exception {

        ValueStateDescriptor<List<SunWordWithCount>> descriptor = new ValueStateDescriptor<List<SunWordWithCount>>("test", TypeInformation.of(new TypeHint<List<SunWordWithCount>>() {}),new ArrayList<SunWordWithCount>());
        sum = getRuntimeContext().getState(descriptor);
    }
}
