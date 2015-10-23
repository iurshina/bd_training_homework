import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import values.LongDoublePair;
import values.LongPair;

/**
 * Counts average bytes count per request by IP and total bytes by IP.
 *
 * @author Anastasiia_Iurshina
 */
public class BytesCounter extends Configured implements Tool {

    public static void main(final String[] args) throws Exception {
        ToolRunner.run(new BytesCounter(), args);
        System.exit(1);
    }

    public int run(final String[] args) throws Exception {
        Configuration configuration = getConf();
        configuration.set("mapred.textoutputformat.separator", ",");
        configuration.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        configuration.set("mapreduce.output.fileoutputformat.compress", "true");
        configuration.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");

        Job job = Job.getInstance(configuration);

        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CounterMapper.class);
        job.setCombinerClass(CounterCombiner.class);
        job.setReducerClass(CounterReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongPair.class);
        job.setOutputValueClass(LongDoublePair.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //TODO: doesn't work with SequenceFileOutputFormat?!
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
