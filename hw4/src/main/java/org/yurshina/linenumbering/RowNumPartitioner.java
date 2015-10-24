package org.yurshina.linenumbering;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Anastasiia_Iurshina
 */
public class RowNumPartitioner extends Partitioner<Text, RowNumWritable> {

    @Override
    public int getPartition(final Text key, final RowNumWritable value, final int numPartitions) {
        if (key.toString().equals(RowNumMapper.PARTITIONS_INFO)) {
            return value.getPartition();
        } else {
            String fileName = key.toString();
            int fileIndex = Integer.valueOf(fileName.substring(fileName.length() - 6, fileName.length() - 4));

            return (Integer.valueOf(fileIndex).hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}
