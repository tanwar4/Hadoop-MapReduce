package cs6240.neu.pagerank;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/*
 * Mapper class : emits the node and outlinks data (which is cleaned before emitting)
 * */

public class PreProcessingMapper extends Mapper<Object, Text, Text, Text> {
    
	private static Logger log = Logger.getLogger(PreProcessingMapper.class.getName());
        @Override
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException{
        	String line = values.toString();
        	
        	Bz2WikiParser parse = new Bz2WikiParser();        	
        	ParsedObject record = parse.mainParser(line); //Decompressing , parsing ,cleaning and storing the data from each adjacency list
        	if(record.getKey() != null){
        		Text node = new Text(record.getKey());
        		Text outlinks = new Text(record.getOutlinks());
        		context.write(node,outlinks);
        	}

        }
    
}
