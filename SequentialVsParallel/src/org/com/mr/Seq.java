package org.com.mr;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.com.mr.LoaderRoutine;;
public class Seq {
	//data structure which will contain individual station name as key and list of temperatures as values 
	static HashMap<String,ArrayList<Double>> accumulation_data = new HashMap<>();

	//structure to hold final output ; station id as key and avg temp as value
	static HashMap<String,Double> output = new HashMap<>();

	public static void main(String[] args) {
        
		String inputfile = args[0];
		int version= Integer.parseInt(args[1]);
		// Loading data from Loader Routine into List data structure
		List<String> outputfile = LoaderRoutine.readfile(inputfile);

		//loop through outputfile produced by LoaderRoutine and extract relevant info
		int i=0;
		double[] arr = new double[10]; //calc min max avg
		while(i<10){  

			//Split the output file data structure , extract the TMAX parameter
			//load the extracted information into custom HashMap Datastructure
			long start= System.currentTimeMillis();
			for(String str:outputfile){
				String[] tokens = str.split("[,]");
				if(tokens[2].equalsIgnoreCase("TMAX") && !(tokens[3].isEmpty())){  //extract only records which has T-MAX
					int temp = Integer.parseInt(tokens[3]);
					//fill the data structure with station Id as key and values as an arraylist
					//which contains the count and total temperature

					//if station id not present add the station id and initialize the value as arraylist with two values
					//the first value is no of station Id of Key type
					//the second value is total running sum of Temp 

					//if station id not present add it and initialize the arraylist with 0

					if(version == 1){
						fibonacci(17);
					}
					accumulation_data.putIfAbsent(tokens[0], new ArrayList<Double>(Collections.nCopies(2, 0.0))); //value is arraylist initialized with 0
					accumulation_data.get(tokens[0])
									 .set(0,accumulation_data.get(tokens[0]).get(0)+1);  //increment count  of station id  at the 0 index
					accumulation_data.get(tokens[0])
					                 .set(1,accumulation_data.get(tokens[0]).get(1)+ temp); // add the temp to total running sum  at 1 index

				}
			}       

			//print the average temperature
			for(Map.Entry<String, ArrayList<Double>> entry :accumulation_data.entrySet()){
				String Key = entry.getKey();             // station ID
				double count = entry.getValue().get(0);  //total count
				double temp_sum = entry.getValue().get(1); //total sum of all values
				output.put(Key, ((double)temp_sum)/count);  // storing in output map  
			}
			long stop= System.currentTimeMillis();
			double time=(stop-start)*.001;
			arr[i]=time;
			i++;
		}
		
		//calculation of min max and average
		double min=arr[0],max=arr[0],sum=0;
		for(int j=0;j<9;j++){
			if(arr[j]<min) min=arr[j];
			if(arr[j]>max) max=arr[j];
			sum+=arr[j];
		}
		
		System.out.println("Seq : Min:"+min +" Max: "+max +" Avg"+sum/10);
	}

	private static int fibonacci(int n) {
		if (n <= 1) return n;
		else return fibonacci(n-1) + fibonacci(n-2);

	}

}
