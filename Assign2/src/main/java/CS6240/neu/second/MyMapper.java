package CS6240.neu.second;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/*
 * Mapper class output key is custom writable key and value is also a custom writable class
 * */

public class MyMapper extends Mapper<Object, Text, CompositeKeyWritable, ValueWritable> {
    
	
	private static Logger log = Logger.getLogger(MyMapper.class.getName());
        @Override
        public void map(Object key, Text values, Context context)
        {
        	
        	   String line = values.toString();
               String tokens[] = line.split("[,]");  
               
             if(tokens.length>0){
                if(!StringUtils.isEmpty(tokens[3]) && ((tokens[2].equalsIgnoreCase("TMAX"))||(tokens[2].equalsIgnoreCase("TMIN")))){
				                try {
				                    int year = Integer.parseInt(tokens[1].substring(0, 4));
				                    //create custom writable key comprising of station Id , year and temptype
				                    CompositeKeyWritable cw = new CompositeKeyWritable(tokens[0],tokens[2],year);
				                    
				                    //custome writable value which store temp_type, temperature and year
				                    ValueWritable vw = new ValueWritable(tokens[2],Float.parseFloat(tokens[3]), year);
				                    context.write(cw, vw);
				                } catch (Exception ex) {
				                    Logger.getLogger(MyMapper.class.getName()).log(Level.SEVERE, null, ex);
				                }
				}
             }
        }
    
}
