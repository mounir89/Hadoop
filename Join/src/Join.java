


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Join {
        
 public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
	 

	private Text word = new Text();
	 
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
       
    	String line = value.toString();
    	
    	String[] record = line.split(",");
    	
    	int keyJoin = Integer.valueOf(record[1]);
    	
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
        	word.set(tokenizer.nextToken());
            context.write(new IntWritable(keyJoin), word);
           
        }
       
      
    }
   
 } 
        
 public static class Reduce extends Reducer<IntWritable,Text, IntWritable,Text> {


    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
    	
    	 ArrayList<HashMap<Integer, LinkedList<String>>> R = new ArrayList<HashMap<Integer,LinkedList<String>>>();
    	 ArrayList<HashMap<Integer, LinkedList<String>>> S = new ArrayList<HashMap<Integer,LinkedList<String>>>();
    	
    	for (Text val : values) {
             
    		String[] record = val.toString().split(",");
             if(record[0].equalsIgnoreCase("R")){
            	 LinkedList<String> tm = new LinkedList<String>();
            	 for(String s : record) {
                    tm.add(s);
                 }
            	 
            	 HashMap<Integer, LinkedList<String>> temp = new HashMap<Integer, LinkedList<String>>();
            	 temp.put(key.get(), tm);
            	 R.add(temp);
            	 
             }else if(record[0].equalsIgnoreCase("S")){
            	 LinkedList<String> tm = new LinkedList<String>();
            	 for(String s : record) {
                    tm.add(s);
                 }
            	 HashMap<Integer, LinkedList<String>> temp = new HashMap<Integer, LinkedList<String>>();
            	 temp.put(key.get(), tm);
            	 S.add(temp);
             }
             
        }
    	
    	
    	
    	for(HashMap<Integer, LinkedList<String>> rr : R){
    		for(Entry<Integer, LinkedList<String>> er : rr.entrySet()){
    			for(HashMap<Integer, LinkedList<String>> ss : S){
    	    		for(Entry<Integer, LinkedList<String>> es : ss.entrySet()){
    	    			 
    	    			 LinkedList<String> temp = new LinkedList<String>();
    	    			 temp.add(er.getValue().get(0)+""+es.getValue().get(0));
    	    		     temp.add(String.valueOf(er.getValue().get(1)));
    	    		     
    	    		     for(int i =2; i<er.getValue().size();i++){
    	    		    	 
    	    		    	 temp.add(String.valueOf(er.getValue().get(i)));
    	    		     }
    	    		     for(int i =2; i<es.getValue().size();i++){
    	    		    	 
    	    		    	 temp.add(String.valueOf(es.getValue().get(i)));
    	    		     }
    	    		     
    	    		    
    	    		     context.write(null,new Text(temp.toString().replace("[", "").replace("]", "")));  
    	    		}
    	    	}
    		}
    	}
    	
    } 	
 }
     
 
 public static void main(String[] args) throws Exception {
	 
    Configuration conf = new Configuration();
   
    Job job = new Job(conf, "Join");
  
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    System.out.println("Join");    
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setNumReduceTasks(1);
    
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.waitForCompletion(true);   
}
        
}
