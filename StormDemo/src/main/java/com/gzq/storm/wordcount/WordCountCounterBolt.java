package com.gzq.storm.wordcount;

import com.gzq.storm.utils.NCUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * ${DESCRIPTION}
 *
 * @author guozhiqiang
 * @created 2017-12-26 15:44.
 */
public class WordCountCounterBolt implements IRichBolt {

    private TopologyContext context;
    private OutputCollector collector;

    private Map<String, Integer> map;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        NCUtil.sendMessageToClient(this, "prepare()");

        this.collector = collector;
        this.context = context;
        map = new HashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple input) {
        NCUtil.sendMessageToClient(this, "execute(" + input.toString() + ")");

        String word = input.getString(0);
        Integer count = input.getInteger(1);
        if (!map.containsKey(word)) {
            map.put(word, count);
        } else {
            map.put(word, map.get(word) + count);
        }

    }


    @Override
    public void cleanup() {
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("word", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
