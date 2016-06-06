package com.lukevinton.storm.imageClassifier;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;

public class TwitterReplyBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input)  {
        String classifications = (String) input.getValueByField("classifications");
        Status tweet = (Status) input.getValueByField("tweet");
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.setUseSSL(true);
        builder.setUser("imageclassifier");
        Twitter factory = new TwitterFactory(builder.build()).getInstance();
        StatusUpdate reply = new StatusUpdate(getBestGuess(classifications));
        reply.inReplyToStatusId(tweet.getId());

        try {
            factory.updateStatus(reply);
        }
        catch(TwitterException e) {
            System.out.println(e.getMessage());
        }
        collector.emit(new Values(tweet, input.getValueByField("img"), classifications));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet", "img", "classifications"));
    }

    private String getBestGuess(String classifications) {
        String[] arrClassifications = classifications.split("\n");
        return  "It's a " + arrClassifications[0];
    }

}
