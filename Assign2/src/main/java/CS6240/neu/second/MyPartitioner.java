package CS6240.neu.second;

import org.apache.hadoop.mapreduce.Partitioner;
/*
 * Partioner class which partition the intermediate values based on station ID such that all values with same station Id goes to 
 * same reducer
 * */
public class MyPartitioner extends Partitioner<CompositeKeyWritable, ValueWritable>{

    public int getPartition(CompositeKeyWritable key, ValueWritable value, int numOfPartitions) {
        
        return ((key.getStationId().hashCode() & Integer.MAX_VALUE) % numOfPartitions);
       
    }
}
