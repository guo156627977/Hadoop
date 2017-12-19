package com.gzq.storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * ${DESCRIPTION}
 *
 * @author guozhiqiang
 * @created 2017-12-18 16:19.
 */
public class LogAnalyserStorm {

    public static void main(String[] args) throws Exception {



        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("call-log-reader-spout", new CallLogSpout());

        builder.setBolt("call-log-creator-bolt", new CallLogCreatorBolt())
                .shuffleGrouping("call-log-reader-spout");

        builder.setBolt("call-log-counter-bolt", new CallLogCounterBolt())
                .fieldsGrouping("call-log-creator-bolt", new Fields("call"));

        Config config = new Config();
        config.setDebug(true);
        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("LogAnalyserStorm", config, builder.createTopology());
        //Thread.sleep(10000);

        //cluster.shutdown();


        StormSubmitter.submitTopology("LogAnalyserStorm", config, builder.createTopology());


    }
}
