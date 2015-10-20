import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Anastasiia_Iurshina
 */
public class RequestMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    enum LineFormat {
        UNEXPECTED_LINE_LENGTH,
        NO_NUMBER
    }

    enum Browser {
        MOZILLA("Mozilla"),
        SAFARI("Safari"),
        OPERA("Opera"),
        OTHERS("");

        private String name;

        Browser(final String name) {
            this.name = name;
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s+");

        if (tokens.length < 9) {
            System.err.println("Unexpected line length: " + key.toString());
            context.setStatus("Detected possibly corrupt record: see logs.");
            context.getCounter(LineFormat.UNEXPECTED_LINE_LENGTH).increment(1);
            return;
        }

        try {
//            if (tokens[11].startsWith(Browser.MOZILLA.name)) {
//                context.getCounter(Browser.MOZILLA).increment(1);
//            } else if (tokens[11].startsWith(Browser.OPERA.name)) {
//                context.getCounter(Browser.OPERA).increment(1);
//            } else if (tokens[11].startsWith(Browser.SAFARI.name)) {
//                context.getCounter(Browser.SAFARI).increment(1);
//            } else {
//                context.getCounter(Browser.OTHERS).increment(1);
//            }

            context.write(new Text(tokens[0]), new LongWritable(Long.valueOf(tokens[9])));
        } catch (NumberFormatException e) {
            System.err.println("Bytes count is not a number: " + key.toString());
            context.setStatus("Detected possibly corrupt record: see logs.");
            context.getCounter(LineFormat.NO_NUMBER).increment(1);
        }
    }
}
