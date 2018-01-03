package com.gzq.storm.wordcount;

import com.gzq.storm.utils.NCUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * ${DESCRIPTION}
 *
 * @author guozhiqiang
 * @created 2017-12-26 15:37.
 */
public class WorldCountSplitBolt implements IRichBolt {
    private TopologyContext context;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        NCUtil.sendMessageToClient(this, "prepare()");

        this.collector = collector;
        this.context = context;

    }

    @Override
    public void execute(Tuple input) {
        NCUtil.sendMessageToClient(this, "execute("+input.toString()+")");

        String line = input.getString(0);
        String[] strings = line.split(" ");
        for (String string : strings) {
            collector.emit(new Values(string, 1));
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
