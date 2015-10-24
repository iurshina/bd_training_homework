package org.yurshina.linenumbering;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Anastasiia_Iurshina
 */
public class RowNumMapper extends Mapper<LongWritable, Text, Text, RowNumWritable> {

    private long[] countPerReducer;
    private int numPartitions;

    public static final String PARTITIONS_INFO = "PARTITIONS";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        numPartitions = context.getNumReduceTasks();
        countPerReducer = new long[numPartitions];
    }

    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
        FileSplit split = ((FileSplit) context.getInputSplit());
        String fileName = split.getPath().getName();

        RowNumWritable r = new RowNumWritable();
        r.setValue(value);
        context.write(new Text(fileName), r);

        int fileIndex = Integer.valueOf(fileName.substring(fileName.length() - 6, fileName.length() - 4));

        countPerReducer[(Integer.valueOf(fileIndex).hashCode() & Integer.MAX_VALUE) % numPartitions]++;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (int i = 0; i < countPerReducer.length - 1; i++) {
            if (countPerReducer[i] > 0) {
                RowNumWritable r = new RowNumWritable();
                r.setCounter(i + 1, countPerReducer[i]);
                context.write(new Text(PARTITIONS_INFO), r);
            }
            countPerReducer[i + 1] += countPerReducer[i];
        }
    }
}
