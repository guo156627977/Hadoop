package com.gzq.storm.wordcount;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * ${DESCRIPTION}
 *
 * @author guozhiqiang
 * @created 2017-12-26 15:58.
 */
public class App {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wcSpout", new WordCountSpout());
        builder.setBolt("wcSplit", new WorldCountSplitBolt()).shuffleGrouping("wcSpout");
        builder.setBolt("wcCountor", new WordCountCounterBolt()).fieldsGrouping("wcSplit", new Fields("word"));

        Config config = new Config();
        config.setDebug(true);
        ////本地模式
        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("LogAnalyserStorm", config, builder.createTopology());
        //Thread.sleep(10000);
        //cluster.shutdown();

        //集群模式
        StormSubmitter.submitTopology("wc", config, builder.createTopology());

    }
}
