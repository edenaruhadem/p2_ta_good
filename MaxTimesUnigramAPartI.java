import java.io.IOException;
import java.util.StringTokenizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Set;

import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTimesUnigramAPartI
{
    public static class MaxTimesUnigramAMapper extends Mapper<Object, Text, Text, Text>
    {
        private Text year = new Text();       
        private Text unitimes = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{                   
        String [] partsLine = value.toString().trim().split("\t");
        String unigram = partsLine[0];       
        String stryear = partsLine[1];
        String times = partsLine[2];
        Integer y = Integer.parseInt(stryear);
        if(y>=1800)
        {
        Integer lastnumy = Integer.parseInt(stryear.substring(stryear.length()-1));
        Integer decade = y + (10-lastnumy);
        String strdecade = Integer.toString(decade);   
        year.set(strdecade);
        unitimes.set(unigram+"/"+times);
        context.write(year, unitimes);       
        }
                         
        }
    }


   





    public static class MaxTimesUnigramAReducer extends Reducer<Text, Text, Text, Text>
    {
       
        private Text unigram = new Text();
        HashMap<String, Integer> map =  new HashMap<String, Integer>();
       
        public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {

            Integer maxValue = 0;           
            for (Text val : values)
            {
                String[] parts = val.toString().split("/");
                String uni = parts[0];
        String t = parts[1];       
                Integer time = Integer.parseInt(t);
        //System.out.println(uni+"/"+t);
                map.put(uni, time);               
            }
            Map.Entry<String,Integer> maxEntry =  getMaxEntry(map);
        String uni = maxEntry.getKey();                  
            unigram.set(uni);
        map.clear();
            context.write(key, unigram);           
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "maxunigramAPartI");
        job.setJarByClass(MaxTimesUnigramAPartI.class);

        job.setMapperClass(MaxTimesUnigramAMapper.class);
        //job.setCombinerClass(MaxTimesUnigramACombiner.class);
        job.setReducerClass(MaxTimesUnigramAReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));               
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
  

    public static Entry<String, Integer> getMaxEntry(HashMap<String, Integer> map){      
        Entry<String, Integer> maxEntry = null;
        Integer max = Collections.max(map.values());  
        for(Entry<String, Integer> entry : map.entrySet()) {
            Integer value = entry.getValue();
            if(null != value && max == value) {
                maxEntry = entry;
            }
        }
        return maxEntry;
    }

}

   
   
   

	
	
	
