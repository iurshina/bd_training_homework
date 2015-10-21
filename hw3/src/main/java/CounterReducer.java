import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Anastasiia_Iurshina
 */
public class CounterReducer extends Reducer<Text, IntDoublePair, Text, IntDoublePair> {

    @Override
    protected void reduce(final Text key, final Iterable<IntDoublePair> values, final Context context) throws IOException, InterruptedException {
        long sum = 0;
        double count = 0;
        for (IntDoublePair value : values) {
            sum += value.getLongValue();
            count += value.getDoubleValue();
        }

        context.write(key, new IntDoublePair(sum, sum / count));
    }
}
