package org.yurshina.linenumbering;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author Anastasiia_Iurshina
 */
public class RowNumOutputValueGroupingComparator extends WritableComparator {

    public RowNumOutputValueGroupingComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(final WritableComparable wc1, final WritableComparable wc2) {
        return 0;
    }
}
