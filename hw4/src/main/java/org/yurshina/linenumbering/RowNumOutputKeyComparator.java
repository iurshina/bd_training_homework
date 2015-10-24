package org.yurshina.linenumbering;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author Anastasiia_Iurshina
 */
public class RowNumOutputKeyComparator extends WritableComparator {

    public RowNumOutputKeyComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(final WritableComparable wc1, final WritableComparable wc2) {
        return wc1.toString().compareTo(wc2.toString());
    }
}
