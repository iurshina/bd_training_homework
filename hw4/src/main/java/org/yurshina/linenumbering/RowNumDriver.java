package org.yurshina.linenumbering;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Numerates rows in the files from the provided directory.
 *
 * @author Anastasiia_Iurshina
 */
public class RowNumDriver extends Configured implements Tool {

    public static void main(final String[] args) throws Exception {
        System.exit(ToolRunner.run(new RowNumDriver(), args));
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        job.setMapperClass(RowNumMapper.class);
        job.setSortComparatorClass(RowNumOutputKeyComparator.class);

        job.setPartitionerClass(RowNumPartitioner.class);
        job.setReducerClass(RowNumReducer.class);
        job.setNumReduceTasks(9);

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RowNumWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
