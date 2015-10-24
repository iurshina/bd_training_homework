package org.yurshina.linenumbering;

import static org.yurshina.linenumbering.Utils.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * If the key indicates that it's an offset info object, returns the number of reducer this info is for,
 * otherwise calculates the number of reducer based on the file name.
 *
 * @author Anastasiia_Iurshina
 */
public class RowNumPartitioner extends Partitioner<Text, RowNumWritable> {

    @Override
    public int getPartition(final Text key, final RowNumWritable value, final int numPartitions) {
        if (key.toString().equals(PARTITIONS_INFO)) {
            return value.getPartition();
        } else {
            return hashByFileName(key.toString(), numPartitions);
        }
    }
}
