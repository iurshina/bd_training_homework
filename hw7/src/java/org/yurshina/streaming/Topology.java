package org.yurshina.streaming;

import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Anastasiia_Iurshina
 */
public class Topology {

    public static final String KAFKA_SPOUT_ID = "kafka-spout";
    public static final String LOGS_PROCESS_BOLT_ID = "logs-process-bolt";
    public static final String HIVE_BOLT_ID = "hive-logs-bolt";

    public static void main(String... args) {
        Topology app = new Topology();
        app.run(args);
    }

    public void run(String... args){
        String kafkaTopic = "logs_topic";

        SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts("127.0.0.1"), kafkaTopic, "/kafka_storm", "StormSpout");
        spoutConfig.useStartOffsetTimeIfOffsetOutOfRange = true;
        spoutConfig.startOffsetTime = System.currentTimeMillis();

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        String metaStoreURI = "thrift://one.hdp:9083";
        String dbName = "default";
        String tblName = "logs10";
        String[] partNames = {"log"};
        String[] colNames = {"log"};
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
                .withColumnFields(new Fields(colNames))
                .withPartitionFields(new Fields(partNames));

        HiveOptions hiveOptions;
        hiveOptions = new HiveOptions(metaStoreURI, dbName, tblName, mapper)
                .withTxnsPerBatch(2)
                .withBatchSize(100)
                .withIdleTimeout(10)
                .withCallTimeout(10000000);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(KAFKA_SPOUT_ID, kafkaSpout);
        builder.setBolt(LOGS_PROCESS_BOLT_ID, new LogDataBolt()).shuffleGrouping(KAFKA_SPOUT_ID);
        builder.setBolt(HIVE_BOLT_ID, new HiveBolt(hiveOptions)).shuffleGrouping(LOGS_PROCESS_BOLT_ID);

        String topologyName = "StormHiveStreamingTopol";
        Config config = new Config();
        config.setNumWorkers(1);
        config.setMessageTimeoutSecs(60);
        try {
            StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
        } catch (AuthorizationException ex) {
            Logger.getLogger(Topology.class.getName()).log(Level.SEVERE, null, ex);
        } catch (AlreadyAliveException ex) {
            Logger.getLogger(Topology.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InvalidTopologyException ex) {
            Logger.getLogger(Topology.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
