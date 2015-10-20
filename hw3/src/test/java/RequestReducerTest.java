import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;

/**
 * @author Anastasiia_Iurshina
 */
public class RequestReducerTest {

    @Test
    public void returnsMaximumIntegerInValues() throws IOException,
                                                       InterruptedException {
        new ReduceDriver<Text, LongWritable, Text, IntDoublePair>()
                .withReducer(new RequestReducer())
                .withInput(new Text("ip1"), Arrays.asList(new LongWritable(400), new LongWritable(200)))
                .withOutput(new Text("ip1"), new IntDoublePair(600, 300))
                .runTest();
    }
}
