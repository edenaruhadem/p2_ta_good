import java.io.IOException;
import java.util.StringTokenizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Set;
import java.io.*;

import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTimesBigramAPartII
{
    public static class MaxTimesBigramAMapper extends Mapper<Object, Text, Text, Text>
    {
        private Text year = new Text();       
        private Text bitimes = new Text();
        HashMap<String, String> mapUnigram =  new HashMap<String, String>();        
	
	public void setup(Context context) throws IOException
	{
          Path pt=new Path("/outputs/part-r-00000");//Location of file in HDFS
          FileSystem fs = FileSystem.get(new Configuration());
          BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
          String line;
          line=br.readLine();
	  String [] parts = line.trim().split("\t");
          String dec = parts[0].trim();
          String unigram = parts[1].trim();
          //System.out.println(dec+unigram);
          mapUnigram.put(dec,unigram);
          do
	  {
            //System.out.println(line);
            line=br.readLine();
	    //System.out.println(line);
	    if(line!=null)
	    {
            	parts = line.split("\t");
            	dec = parts[0].trim();
            	unigram = parts[1].trim();
            	mapUnigram.put(dec,unigram);
	    }
          }while(line!=null);
	  br.close();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
          //System.out.println(value.toString());      
          String [] partsLine = value.toString().trim().split("\t");
          String bigram = partsLine[0].trim();       
          String stryear = partsLine[1].trim();
          String times = partsLine[2].trim();
          //System.out.println(bigram+"/"+stryear+"/"+times);
          Integer y = Integer.parseInt(stryear);
          if(y>=1900)
          {
	    Integer lastnumy = Integer.parseInt(stryear.substring(stryear.length()-1));
	    //Integer lasttwonumy = Integer.parseInt(stryear.substring(2,4));
            Integer decade = y - lastnumy;
	    //Integer decade = y - lasttwonumy;
            //Integer decade = y + (10-lastnumy);
            String strdecade = Integer.toString(decade);
            Map.Entry<String,String> unigramEntry = getUnigramEntry(mapUnigram, strdecade);
            //System.out.println(unigramEntry.getValue()+"/"+unigramEntry.getKey());
	    String unigram = unigramEntry.getValue();
	    String [] splitBigram = bigram.split(" ");
	    Integer lengthUnigram = unigram.length();
	    if(lengthUnigram > 1)
	    {
		String twoLetters = unigram.substring(0,2);		
            	String uni = splitBigram[0].trim();
		if(twoLetters.equals(uni.substring(0,2)))
		{
			String wordBigram = splitBigram[1].trim();
              		//System.out.println(oneuni+"biiiiigram: "+uni+" "+wordBigram);
              		year.set(strdecade);
             		bitimes.set(wordBigram+"@"+times);
              		context.write(new Text(year), new Text(bitimes));			
		}
	    }
	    else
	    {
		//System.out.println(oneuni+"biiiiiiigrammmm"+bigram);            	
            	String uni = splitBigram[0].trim();
            	if(uni.equals(unigram))
            	{
              		String wordBigram = splitBigram[1].trim();
              		//System.out.println(oneuni+"biiiiigram: "+uni+" "+wordBigram);
              		year.set(strdecade);
              		bitimes.set(wordBigram+"@"+times);
              		context.write(new Text(year), new Text(bitimes));
            	}		
	    }             
          }                        
        }
    }

    public static class MaxTimesBigramAReducer extends Reducer<Text, Text, Text, Text>
    {
       
        private Text bigram = new Text();
        HashMap<String, Integer> map =  new HashMap<String, Integer>();       
        public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException 
	{
            Integer maxValue = 0;           
            for (Text val : values)
            {
		//System.out.println(val.toString());
                String[] parts = val.toString().trim().split("@");
                String wordbi = parts[0].trim();
        	String t = parts[1].trim();
		//System.out.println(wordbi);
		if((t.indexOf(".")==-1) && (t.indexOf("_")==-1) && (!t.equals("")) && (!t.equals(null)))
		{       
                  Integer time = Integer.parseInt(t);
		  //System.out.println("word: "+bi+" time: "+t);        
                  map.put(wordbi, time);
		}                               
            }
            Map.Entry<String,Integer> maxEntry =  getMaxEntry(map);
            String bi = maxEntry.getKey();
	    //System.out.println(bi);                  
            bigram.set(bi);
            map.clear();
            context.write(new Text(key), new Text(bigram));           
        }
    }


    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "maxbigram");
        job.setJarByClass(MaxTimesBigramAPartII.class);
        job.setMapperClass(MaxTimesBigramAMapper.class);        
        job.setReducerClass(MaxTimesBigramAReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));       
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
  

    public static Entry<String, Integer> getMaxEntry(HashMap<String, Integer> map)
    {      
        Entry<String, Integer> maxEntry = null;
        Integer max = Collections.max(map.values());	  
        for(Entry<String, Integer> entry : map.entrySet()) 
	{
            Integer value = entry.getValue();	    
            if(null != value && max == value) 
	    {
                maxEntry = entry;		
            }
        }
        return maxEntry;
    }

    public static Entry<String, String> getUnigramEntry(HashMap<String, String> mapUnigram, String decade)
    {      
        Entry<String, String> unigramEntry = null;         
        for(Entry<String, String> entry : mapUnigram.entrySet()) 
	{
            String key = entry.getKey();
            if(decade.equals(key)) 
	    {
                unigramEntry = entry;
            }
        }
        return unigramEntry;
    }
}

   
   
   

	
	
	
