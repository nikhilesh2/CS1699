package org.dataalgorithms.chap07.mapreduce;

import java.io.IOException;
// import java.util.List;
// import java.util.ArrayList;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.log4j.Logger;
import org.apache.commons.lang3.StringUtils;

import org.dataalgorithms.util.Combination;


public class MBAMapper extends Mapper<LongWritable, Text, Text, Text> {

   public static final Logger THE_LOGGER = Logger.getLogger(MBAMapper.class);

   public static final int DEFAULT_NUMBER_OF_PAIRS = 2; 

   //output key2: list of items paired; can be 2 or 3 ...
   private static final Text reducerKey = new Text(); 
   

   int numberOfPairs; // will be read by setup(), set by driver
   
   @Override
   protected void setup(Context context) throws IOException, InterruptedException {
      this.numberOfPairs = context.getConfiguration().getInt("number.of.pairs", DEFAULT_NUMBER_OF_PAIRS);
      THE_LOGGER.info("setup() numberOfPairs = " + numberOfPairs);
      System.out.println("setup() numberOfPairs = " + numberOfPairs);
    }

   @Override
   public void map(LongWritable key, Text value, Context context) 
      throws IOException, InterruptedException {

      // input line
      String line = value.toString();
      List<String> items = convertItemsToList(line);
      if ((items == null) || (items.isEmpty())) {
         // no mapper output will be generated
         return;
      }
      generateMapperOutput(numberOfPairs, items, context);
   }
   
   private static List<String> convertItemsToList(String line) {
      if ((line == null) || (line.length() == 0)) {
         // no mapper output will be generated
         return null;
      }
      String[] tokens = StringUtils.split(line, " ");  
      if ( (tokens == null) || (tokens.length == 0) ) {
         return null;
      }
      List<String> items = new ArrayList<String>();    
      for (String token : tokens) {
         if (token != null) {
             items.add(token.trim());
         }         
      }        
      items.remove(0);
      return items;
   }
   

   private void generateMapperOutput(int numberOfPairs, List<String> items, Context context) 
      throws IOException, InterruptedException {
      List<List<String>> sortedCombinations = Combination.findSortedCombinations(items, numberOfPairs);
      Map<String, HashMap<String, Integer>> mappings = new HashMap<String, HashMap<String, Integer>>();
      
      for (List<String> itemList: sortedCombinations) {
         System.out.println("itemlist="+itemList.toString());
         THE_LOGGER.info("itemlist="+itemList.toString());
         String first = itemList.get(0);
         String second = itemList.get(1);
         
         if(!mappings.containsKey(first)) {
            mappings.put(first, new HashMap<String, Integer>());
         }
         if(!mappings.containsKey(second)) {
            mappings.put(second, new HashMap<String, Integer>());
         }

         HashMap<String, Integer> m1 = mappings.get(first);
         HashMap<String, Integer> m2 = mappings.get(second);

         if(!m1.containsKey(second)) {
            m1.put(second, 1);
         } else {
            m1.put(second, m1.get(second) + 1);
         }

         if(!m2.containsKey(first)) {
            m2.put(first, 1);
         } else {
            m2.put(first, m2.get(first) + 1);
         }

         mappings.put(first, m1);
         mappings.put(second, m2);

      } 

      // Should write it as like 
      // A "B,2"
      // A "C,1"
      for (String key : mappings.keySet()) {
    	 THE_LOGGER.info("KEY="+key);
         HashMap<String, Integer> m = mappings.get(key);
         for (String key2 : m.keySet()) {
            reducerKey.set(key.replaceAll("\\s+", ""));
            Text val = new Text(key2.replaceAll("\\s+", "") + "," + m.get(key2));
            context.write(reducerKey, val);
         }
      }
   }
   
}

