import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Anastasiia_Iurshina
 */
public class CounterMapper extends Mapper<LongWritable, Text, Text, IntDoublePair> {

    enum LineFormat {
        UNEXPECTED_LINE_LENGTH,
        NO_NUMBER
    }

    enum Browser {
        MOZILLA("mozilla"),
        SAFARI("safari"),
        OPERA("opera"),
        OTHERS("");

        private String name;

        Browser(final String name) {
            this.name = name;
        }
    }

    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s+");

        if (tokens.length < 10) {
            System.err.println("Unexpected line length: " + key.toString());
            context.setStatus("Detected possibly corrupt record: see logs.");
            context.getCounter(LineFormat.UNEXPECTED_LINE_LENGTH).increment(1);
            return;
        }

        try {
            context.getCounter(getCounter(tokens));
            context.write(new Text(tokens[0]), new IntDoublePair(Long.valueOf(tokens[9]), 1));
        } catch (NumberFormatException e) {
            System.err.println("Bytes count is not a number: " + key.toString());
            context.setStatus("Detected possibly corrupt record: see logs.");
            context.getCounter(LineFormat.NO_NUMBER).increment(1);
        }
    }

    //TODO: Use counters to get stats how many users of IE, Mozzila or Other were detected
    private Browser getCounter(final String[] tokens) {
        if (tokens.length < 12) {
            return Browser.OTHERS;
        }

        String token = tokens[11];
        if (tokens[11] == null) {
            return Browser.OTHERS;
        }
        token = token.toLowerCase();

        if (token.contains(Browser.MOZILLA.name)) {
            return Browser.MOZILLA;
        } else if (token.contains(Browser.OPERA.name)) {
            return Browser.OPERA;
        } else if (token.contains(Browser.SAFARI.name)) {
            return Browser.SAFARI;
        }

        return Browser.OTHERS;
    }
}
