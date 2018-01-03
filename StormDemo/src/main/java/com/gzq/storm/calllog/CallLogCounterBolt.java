package com.gzq.storm.calllog;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * ${DESCRIPTION}
 *
 * @author guozhiqiang
 * @created 2017-12-19 16:38.
 */
public class CallLogCounterBolt implements IRichBolt {

    Map<String, Integer> counterMap;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.counterMap = new HashMap<String, Integer>();
        this.collector= collector;
    }

    @Override
    public void execute(Tuple input) {
        String call = input.getString(0);
        Integer duration = input.getInteger(1);
        if (!counterMap.containsKey(call)) {
            counterMap.put(call, 1);
        }else {
            Integer i = counterMap.get(call) + 1;
            counterMap.put(call, i);

        }
        //确认成功
        collector.ack(input);

    }

    /**
     * 结束后所做的事
     */
    @Override
    public void cleanup() {
        for (Map.Entry<String, Integer> entry : counterMap.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("call"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
