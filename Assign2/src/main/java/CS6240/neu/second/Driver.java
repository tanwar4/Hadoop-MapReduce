package CS6240.neu.second;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;


public class Driver {

    /**
     * Main Driver class
     * Number of reducer task is set to 5
     */
    public static void main(String[] args) {
        
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Driver");
            BasicConfigurator.configure(); 
            job.setJarByClass(Driver.class);
           
            job.setMapperClass(MyMapper.class);
            job.setMapOutputKeyClass(CompositeKeyWritable.class);
            job.setMapOutputValueClass(ValueWritable.class);
            
            job.setPartitionerClass(MyPartitioner.class);
            job.setGroupingComparatorClass(GroupComparator.class);
            
            job.setReducerClass(MyReducer.class);
            job.setOutputKeyClass(CompositeKeyWritable.class);
            job.setOutputValueClass(NullWritable.class);
            
            job.setNumReduceTasks(5);
            
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            
            
            
            System.exit(job.waitForCompletion(true) ? 0 : 1);
            
        } catch (Exception ex) {
            System.out.println("Erorr Message"+ ex.getMessage());
        }
    }
    
}
