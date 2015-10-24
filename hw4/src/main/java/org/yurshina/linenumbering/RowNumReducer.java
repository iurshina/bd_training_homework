package org.yurshina.linenumbering;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Anastasiia_Iurshina
 */
public class RowNumReducer extends Reducer<Text, RowNumWritable, Text, Text> {

    @Override
    protected void reduce(final Text key, final Iterable<RowNumWritable> values, final Context context) throws IOException, InterruptedException {
        long offset = 0;
        Iterator<RowNumWritable> iterator = values.iterator();
        RowNumWritable value = iterator.next();
        while (iterator.hasNext() && value.getCount() > 0) {
            offset += value.getCount();
            value = iterator.next();
        }

        while (iterator.hasNext()) {
            if (value.getCount() == 0) {
                context.write(new Text(Long.toString(offset)), value.getValue());
            }
            offset++;
            value = iterator.next();
        }
    }
}
