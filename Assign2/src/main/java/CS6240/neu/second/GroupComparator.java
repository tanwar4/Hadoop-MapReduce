package CS6240.neu.second;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/*
 * Group comparator which does the grouping based on station Id
 * The reducer will get list of values for each station Id
 * */

public class GroupComparator extends WritableComparator {
    
    protected GroupComparator()
    {
        super(CompositeKeyWritable.class, true);
    }
    
    @Override
    public int compare(WritableComparable w1, WritableComparable w2)
    {
        CompositeKeyWritable cw1 = (CompositeKeyWritable) w1;
        CompositeKeyWritable cw2 = (CompositeKeyWritable) w2;
        
           return (cw1.getStationId().compareTo(cw2.getStationId()));
    }
}
