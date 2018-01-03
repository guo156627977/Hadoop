package com.gzq.storm.wordcount;

import com.gzq.storm.utils.NCUtil;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * ${DESCRIPTION}
 *
 * @author guozhiqiang
 * @created 2017-12-26 15:25.
 */
public class WordCountSpout implements IRichSpout {

    private TopologyContext context;
    private SpoutOutputCollector collector;

    private List<String> states;

    private Random r = new Random();

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        NCUtil.sendMessageToClient(this,"open()");
        this.context=context;
        this.collector=collector;
        states.add("Hello Tom");
        states.add("Hello Jerry");
        states.add("Hello Guo Zhiqiang");
        states.add("Hello World");
        states.add("Hello Tomas");
        states.add("Hello Storm");
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        NCUtil.sendMessageToClient(this, "nextTuple()");
        String line = states.get(r.nextInt(6));
        collector.emit(new Values(line));

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
