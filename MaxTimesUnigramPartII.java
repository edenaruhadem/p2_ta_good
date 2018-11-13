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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;

public class MaxTimesUnigramPartII
{
    public static class MaxTimesUnigramIIMapper extends Mapper<Object, Text, Text, Text>
    {
        private Text year = new Text();      
        private Text unitimes = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
    //System.out.println(value.toString());                  
        String [] partsLine = value.toString().trim().split("\t");
        String unigram = partsLine[0];      
        String stryear = partsLine[1];
        String times = partsLine[2];
    /*if(times.indexOf("_")!=-1)
    {
        String [] partsTime = times.split("_");
        times = partsTime[0];       
    }*/
        Integer y = Integer.parseInt(stryear);
        if(y>=1800)
        {
        Integer lastnumy = Integer.parseInt(stryear.substring(stryear.length()-1));
        //Integer decade = y + (10-lastnumy);
    Integer decade = y - lastnumy;
        String strdecade = Integer.toString(decade);  
        year.set(strdecade);
        unitimes.set(unigram+"@"+times);
    //System.out.println(unigram+"/"+times);
        context.write(new Text(year), new Text(unitimes));      
        }
                        
        }
    }
    public static class MaxTimesUnigramIIReducer extends Reducer<Text, Text, Text, Text>
    {
      
        private Text unigram = new Text();
        HashMap<String, Integer> map =  new HashMap<String, Integer>();
      
        public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {

            Integer maxValue = 0;          
            for (Text val : values)
            {
        //System.out.println(val.toString());
                String[] parts = val.toString().trim().split("@");
                String uni = parts[0];
                String t = parts[1];

        /*if(t.indexOf("_")!=-1)
                {
           String [] partst = t.split("_");
               t = partst[0];       
            }*/

        //System.out.println(uni+"@"+t);
        //String prueba = "editorial.ucr.ac.cr";
        if(t.indexOf(".")==-1){      
                Integer time = Integer.parseInt(t);       
                map.put(uni, time);}              
            }
            Map.Entry<String,Integer> maxEntry =  getMaxEntry(map);
            String uni = maxEntry.getKey();
        if(uni.indexOf("_")!=-1)
                {
           String [] partsuni = uni.split("_");
               uni = partsuni[0];       
            }                         
            unigram.set(uni);       
            map.clear();
            context.write(new Text(key), new Text(unigram));          
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "maxunigramPartII");
        job.setJarByClass(MaxTimesUnigramAPartII.class);

        job.setMapperClass(MaxTimesUnigramIIMapper.class);
        //job.setCombinerClass(MaxTimesUnigramACombiner.class);
        job.setReducerClass(MaxTimesUnigramIIReducer.class);
        job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
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