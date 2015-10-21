import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import values.LongDoublePair;
import values.LongPair;

import java.io.IOException;

/**
 * Result:
 *   LongDoublePair longValue   - total count of bytes
 *   LongDoublePair doubleValue - average count of bytes
 *
 * @author Anastasiia_Iurshina
 */
public class CounterReducer extends Reducer<Text, LongPair, Text, LongDoublePair> {

    @Override
    protected void reduce(final Text key, final Iterable<LongPair> values, final Context context) throws IOException, InterruptedException {
        long sum = 0;
        double count = 0;
        for (LongPair value : values) {
            sum += value.getFirst();
            count += value.getSecond();
        }

        context.write(key, new LongDoublePair(sum, ((double) sum) / count));
    }
}
