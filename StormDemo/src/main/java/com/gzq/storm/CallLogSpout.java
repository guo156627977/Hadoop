package com.gzq.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * ${DESCRIPTION}
 *
 * @author guozhiqiang
 * @created 2017-12-18 15:19.
 */
public class CallLogSpout implements IRichSpout {

    //Create instance for SpoutOutputCollector which passes tuples to bolt.
    //Spout输出收集器
    private SpoutOutputCollector collector;
    //是否完成
    private boolean completed = false;

    //上下文
    //Create instance for TopologyContext which contains topology data.
    private TopologyContext context;
    //随机发生器
    //Create instance for Random class.
    private Random randomGenerator = new Random();
    //索引
    private Integer idx = 0;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context = context;
        this.collector = collector;
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

        if (this.idx <= 1000) {
            List<String> mobileNumbers = new ArrayList<String>();
            mobileNumbers.add("1234123401");
            mobileNumbers.add("1234123402");
            mobileNumbers.add("1234123403");
            mobileNumbers.add("1234123404");

            Integer localIdx = 0;
            while (localIdx++ < 100 && this.idx++ < 1000) {
                //取出主叫
                String fromMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
                //取出被叫
                String toMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));

                while (fromMobileNumber == toMobileNumber) {
                    //重新取出被叫
                    toMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
                }
                //设置通话时长
                Integer duration = randomGenerator.nextInt(60);

                //输出元组
                this.collector.emit(new Values(fromMobileNumber, toMobileNumber, duration));
            }
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
        declarer.declare(new Fields("from", "to", "duration"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
