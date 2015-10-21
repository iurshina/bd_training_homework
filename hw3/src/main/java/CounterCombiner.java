import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import values.LongPair;

import java.io.IOException;

/**
 * Result:
 *   LongPair first  - count of bytes
 *   LongPair second - count
 *
 * @author Anastasiia_Iurshina
 */
public class CounterCombiner extends Reducer<Text, LongPair, Text, LongPair> {

    @Override
    protected void reduce(final Text key, final Iterable<LongPair> values, final Context context) throws IOException, InterruptedException {
        long sum = 0;
        long count = 0;
        for (LongPair value : values) {
            sum += value.getFirst();
            count += value.getSecond();
        }

        context.write(key, new LongPair(sum, count));
    }
}
