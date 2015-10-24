package org.yurshina.linenumbering;

import static org.yurshina.linenumbering.Utils.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Takes the offset in the first calls (since keys are sorted in a way to put key indicating
 * offset info first) in the following calls which contains data from one of the files uses the offset.
 *
 * @author Anastasiia_Iurshina
 */
public class RowNumReducer extends Reducer<Text, RowNumWritable, Text, Text> {

    private long offset;

    @Override
    protected void reduce(final Text key, final Iterable<RowNumWritable> values, final Context context) throws IOException, InterruptedException {
        if (key.toString().equals(PARTITIONS_INFO)) {
            for (RowNumWritable row : values) {
                offset += row.getOffset();
            }
        } else {
            for (RowNumWritable row : values) {
                context.write(new Text(Long.toString(offset)), row.getLine());
                offset++;
            }
        }
    }
}
