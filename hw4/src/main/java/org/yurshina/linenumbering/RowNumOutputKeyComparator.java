package org.yurshina.linenumbering;

import static org.yurshina.linenumbering.Utils.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Sorting keys, puts the ones that contain info about an offset first.
 *
 * @author Anastasiia_Iurshina
 */
public class RowNumOutputKeyComparator extends WritableComparator {

    public RowNumOutputKeyComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(final WritableComparable wc1, final WritableComparable wc2) {
        if (wc1.toString().equals(PARTITIONS_INFO))
            return -1;

        return 0;
    }
}
