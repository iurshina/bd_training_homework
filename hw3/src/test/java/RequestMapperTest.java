import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.*;

/**
 * @author Anastasiia_Iurshina
 */
public class RequestMapperTest {

    @Test
    public void processesValidRecord() throws IOException, InterruptedException {
        Text value = new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 "
                              + "(compatible; YandexImages/3.0; +http://yandex.com/bots)");
        new MapDriver<LongWritable, Text, Text, LongWritable>()
                .withMapper(new RequestMapper())
                .withInput(new LongWritable(0), value)
                .withOutput(new Text("ip1"), new LongWritable(40028))
                .runTest();
    }

    @Test
    public void processesNotNumberRecord() throws IOException, InterruptedException {
        Text value = new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 - \"-\" \"Mozilla/5.0 "
                              + "(compatible; YandexImages/3.0; +http://yandex.com/bots)");
        Counters counters = new Counters();
        new MapDriver<LongWritable, Text, Text, LongWritable>()
                .withMapper(new RequestMapper())
                .withInput(new LongWritable(0), value)
                .withCounters(counters)
                .runTest();
        Counter c = counters.findCounter(RequestMapper.LineFormat.NO_NUMBER);
        assertThat(c.getValue(), is(1L));
    }
}
