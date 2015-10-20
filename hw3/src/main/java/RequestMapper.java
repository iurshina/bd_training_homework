import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Anastasiia_Iurshina
 */
public class RequestMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //TODO: proper parsing
        String[] tokens = value.toString().split("\\s+");

        context.write(new Text(tokens[0]), new LongWritable(Long.valueOf(tokens[9])));
    }
}
