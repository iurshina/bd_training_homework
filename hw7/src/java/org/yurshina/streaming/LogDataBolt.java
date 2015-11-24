package org.yurshina.streaming;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Anastasiia_Iurshina
 */
public class LogDataBolt extends BaseBasicBolt {

    public void declareOutputFields(OutputFieldsDeclarer ofDeclarer) {
        ofDeclarer.declare(new Fields("log"));
    }

    public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
        Fields fields = tuple.getFields();
        try {
            String logStr = new String((byte[]) tuple.getValueByField(fields.get(0)), "UTF-8");
            Values values = new Values(logStr);
            outputCollector.emit(values);
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(Topology.class.getName()).log(Level.SEVERE, null, ex);
            throw new FailedException(ex.toString());
        }
    }
}
