import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTimesUnigramA
{
    static class MaxTimesUnigramAMapper extends Mapper<Object, Text, Text, Text>
    {   
        public void map(Object key, Text value, Context context)throws IOException, InterruptedException                                               
        {
            //LongWritable: tipo Long(dato hadoop). Text: tipo String(dato Hadoop)
            String line = value.toString();
            String unigram = null;
            String year = null;
            String times = null;
            Boolean isUnigram = false;
            Boolean isYear = false;
            Boolean isTime = false;
            StringTokenizer tokens = new StringTokenizer(line);
            while(tokens.hasMoreTokens())
            {
                if(!tokens.nextToken().equals(" "))
                {
                    if(!isUnigram)
                    {
                        unigram = tokens.nextToken();
                        isUnigram = true;
                    }              
                    if(isUnigram && !isYear)
                    {
                        year = tokens.nextToken();
                        isYear = true;
                    }
                    else
                    {
                        times = tokens.nextToken();
                        isTime = true;
                    }
                }           
            }       
            if (Integer.parseInt(year)>=1800)
            {
                context.write(new Text(year), new Text(unigram+"/"+times));
            }        
        }
    }

    static class MaxTimesUnigramAReducer extends Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException
        {
            int maxValue = 0;
            //Integer year = Integer.parseInt(key);           
            //ArrayList<String> years = new ArrayList<String>();
            //ArrayList<String> valueYear = new ArrayList<String>();
            HashMap<String, Integer> map =  new HashMap<String, Integer>();
            //years.add(key);           
            for (Text val : values)
            {
                String[] parts = val.toString().split("/");
                String uni = parts[0];
                Integer time = Integer.parseInt(parts[1]);                
                map.put(uni, time);              
            }
            Map.Entry<String,Integer> maxEntry =  getMaxEntry(map);

            context.write(key, new Text(maxEntry.getKey()));
        }
    }


    public static void main(String[] args) throws Exception
    {       
    Path ruta_entrada = new Path("input");
    Path ruta_salida= new Path("/output/outputP2");
        Job job = new Job();
        job.setJarByClass(MaxTimesUnigramA.class);
        FileInputFormat.addInputPath(job, ruta_entrada);
        FileOutputFormat.setOutputPath(job, ruta_salida);
        job.setMapperClass(MaxTimesUnigramAMapper.class);
        //job.setCombinerClass(MaxTimesUnigramAReducer.class);
        job.setReducerClass(MaxTimesUnigramAReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
    //FileInputFormat.addInputPath(Job,ruta_entrada);
    //FileOutputFormat.addOutputPath(Job,ruta_salida);
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




