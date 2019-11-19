package org.dataalgorithms.chap07.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class MBAReducer extends Reducer<Text, Text, Text, Text>{
	
	public static final Logger THE_LOGGER = Logger.getLogger(MBAReducer.class);
   @Override
   public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
      HashMap<String, Integer> counts = new HashMap<String, Integer>();
      
      for (Text value : values) {
    	  String valStr = value.toString();
    	  if(!counts.containsKey(valStr)) counts.put(valStr,1);
    	  else counts.put(valStr, counts.get(valStr) + 1);
      }
      ArrayList<String> list = new ArrayList<String>();
      for (String theKey : counts.keySet()) {
    	  list.add("(" + theKey.charAt(0) + ", " + counts.get(theKey) + ")");
      }
      String listString = "{";
      for (String s : list)
      {
          listString += s + ", ";
      }
     String finalValue = listString.substring(0, listString.length() - 2) + "}";
     context.write(key, new Text(finalValue));
   }
}

