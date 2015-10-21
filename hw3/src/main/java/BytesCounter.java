import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Anastasiia_Iurshina
 */
public class BytesCounter extends Configured implements Tool {

    public static void main(final String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new BytesCounter(), args);
        System.exit(1);
    }

    public int run(final String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("mapred.textoutputformat.separator", ",");

        Job job = Job.getInstance(configuration);

        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        job.setMapperClass(CounterMapper.class);
        job.setCombinerClass(CounterCombiner.class);
        job.setReducerClass(CounterReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntDoublePair.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
