import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Anastasiia_Iurshina
 */
public class RequestReducer extends Reducer<Text, LongWritable, Text, IntDoublePair> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        int count = 0;
        for (LongWritable value : values) {
            sum += value.get();
            count++;
        }

        context.write(key, new IntDoublePair(sum, (double) sum / count));
    }
}
