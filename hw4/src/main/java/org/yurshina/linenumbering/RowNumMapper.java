package org.yurshina.linenumbering;

import static org.yurshina.linenumbering.Utils.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Output values are either plain text rows or objects containing info about offsets
 * for each reducer. The objects are sent to the corresponding reducers by the partitioner.
 *
 * @author Anastasiia_Iurshina
 */
public class RowNumMapper extends Mapper<LongWritable, Text, Text, RowNumWritable> {

    private int numPartitions;
    private long[] countPerReducer;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        numPartitions = context.getNumReduceTasks();
        countPerReducer = new long[numPartitions];
    }

    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
        FileSplit split = ((FileSplit) context.getInputSplit());
        String fileName = split.getPath().getName();

        RowNumWritable r = new RowNumWritable();
        r.setLine(value);
        context.write(new Text(fileName), r);

        countPerReducer[hashByFileName(fileName, numPartitions)]++;
    }

    /**
     * For each reducer calculates how many lines have been sent to the previous reducers.
     */
    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
        for (int i = 0; i < countPerReducer.length - 1; i++) {
            if (countPerReducer[i] > 0) {
                RowNumWritable r = new RowNumWritable();
                r.setOffset(countPerReducer[i]);
                r.setPartition(i + 1);
                context.write(new Text(PARTITIONS_INFO), r);
                countPerReducer[i + 1] += countPerReducer[i];
            }
        }
    }
}
