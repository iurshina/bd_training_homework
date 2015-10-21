import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import values.LongPair;

import java.io.IOException;
import java.util.Arrays;

/**
 * Test for {@code CounterCombiner}.
 *
 * @author Anastasiia_Iurshina
 */
public class CounterCombinerTest {

    @Test
    public void normalScenarioTest() throws IOException, InterruptedException {
        new ReduceDriver<Text, LongPair, Text, LongPair>()
                .withReducer(new CounterCombiner())
                .withInput(new Text("ip1"), Arrays.asList(new LongPair(600, 1), new LongPair(400, 1)))
                .withOutput(new Text("ip1"), new LongPair(1000, 2))
                .runTest();
    }
}
