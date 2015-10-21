import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;

import values.LongDoublePair;
import values.LongPair;

/**
 * Test for {@code CounterReducer}.
 *
 * @author Anastasiia_Iurshina
 */
public class CounterReducerTest {

    @Test
    public void normalScenarioTest() throws IOException, InterruptedException {
        new ReduceDriver<Text, LongPair, Text, LongDoublePair>()
                .withReducer(new CounterReducer())
                .withInput(new Text("ip1"), Arrays.asList(new LongPair(600, 2), new LongPair(400, 2)))
                .withOutput(new Text("ip1"), new LongDoublePair(1000, 250))
                .runTest();
    }
}
