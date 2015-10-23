import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import values.LongPair;

import java.io.IOException;

/**
 * Result:
 *   LongPair first  - count of bytes
 *   LongPair second - count
 *
 * @author Anastasiia_Iurshina
 */
public class CounterMapper extends Mapper<LongWritable, Text, Text, LongPair> {

    private static final Log LOG = LogFactory.getLog(CounterMapper.class);

    enum LineFormat {
        UNEXPECTED_LINE_LENGTH,
        NO_NUMBER
    }

    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s+");

        if (tokens.length < 10) {
            LOG.error("Unexpected line length: " + key.toString());
            context.setStatus("Detected possibly corrupt record: see logs.");
            context.getCounter(LineFormat.UNEXPECTED_LINE_LENGTH).increment(1);
            return;
        }

        try {
            final long bytes = Long.parseLong(tokens[9]);

            context.getCounter(Browser.getCounter(tokens)).increment(1);;
            context.write(new Text(tokens[0]), new LongPair(bytes, 1));
        } catch (NumberFormatException e) {
            LOG.error("Bytes count is not a number: " + key.toString());
            context.setStatus("Detected possibly corrupt record: see logs.");
            context.getCounter(LineFormat.NO_NUMBER).increment(1);
        }
    }
}
