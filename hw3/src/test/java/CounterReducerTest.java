import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;

/**
 * Test for {@code CounterReducer}.
 *
 * @author Anastasiia_Iurshina
 */
public class CounterReducerTest {

    @Test
    public void normalScenarioTest() throws IOException,
                                                       InterruptedException {
        new ReduceDriver<Text, IntDoublePair, Text, IntDoublePair>()
                .withReducer(new CounterReducer())
                .withInput(new Text("ip1"), Arrays.asList(new IntDoublePair(600, 2), new IntDoublePair(400, 2)))
                .withOutput(new Text("ip1"), new IntDoublePair(1000, 250))
                .runTest();
    }
}
