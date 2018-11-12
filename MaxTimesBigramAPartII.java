import java.io.IOException;
import java.util.StringTokenizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Set;
//import java.nio.file.FileSystem;
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
    //Path unigramFile = new Path("/output/part-r-00000");
    //Aqui leer archivo y rellenar

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{                  
        Path unigramFile = new Path("/outputPartII/part-r-00000");
        FileSystem fs = FileSystem.get(context.getConfiguration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(unigramFile)));
        try {
           String line;
             line=br.readLine();
        String [] parts = line.split("\t");
        String dec = parts[0];
        String unigram = parts[1];
        //System.out.println(dec+unigram);
        mapUnigram.put(dec,unigram);
             do{ 
               line = br.readLine();
        //System.out.println("eeeeeeeeeeeeeeeeeeooooooooooooooooooo"+line);
        if(line!=null){
        parts = line.split("\t");
        dec = parts[0];
        unigram = parts[1];
        mapUnigram.put(dec,unigram);}
             }while(line != null);
            } finally {
              br.close();
              }
        String [] partsLine = value.toString().trim().split("\t");
        String bigram = partsLine[0];      
        String stryear = partsLine[1];
        String times = partsLine[2];
        //System.out.println(bigram+stryear+times);
        Integer y = Integer.parseInt(stryear);
        if(y>=1900)
        {
        Integer lastnumy = Integer.parseInt(stryear.substring(stryear.length()-1));
    Integer decade = y - lastnumy;
        //Integer decade = y + (10-lastnumy);
        String strdecade = Integer.toString(decade);
        Map.Entry<String,String> unigramEntry = getUnigramEntry(mapUnigram, strdecade);
    Integer sizeUni = unigramEntry.getValue().length();
    if(sizeUni>1)
    {
         String twouni = unigramEntry.getValue().substring(0,1);
         String [] splitBigram = bigram.split(" ");
         String bi = splitBigram[0].substring(0,1);
        if(bi.equals(twouni)){
            String wordBigram = splitBigram[1];
            year.set(strdecade);
                    bitimes.set(wordBigram+"/"+times);
                    context.write(new Text(year), new Text(bitimes));
        }
    }
    else
        {
        String oneuni = unigramEntry.getValue();
        String [] splitBigram = bigram.split(" ");
        String uni = splitBigram[0];
        if(uni.equals(oneuni)){
            String wordBigram = splitBigram[1];
            year.set(strdecade);
                    bitimes.set(wordBigram+"@"+times);
                    context.write(new Text(year), new Text(bitimes));
        }       
    }
        //String twouni = unigramEntry.getValue().substring(0,1);//De ese unigram extraer las dos primeras letras
        //String bi = bigram.substring(0,1);
        //if(bi.equals(twouni))
        //{
            //year.set(strdecade);
            //bitimes.set(bigram+"/"+times);
            //context.write(year, bitimes);          
        //}              
        }                        
        }
    }

    public static class MaxTimesBigramAReducer extends Reducer<Text, Text, Text, Text>
    {
      
        private Text bigram = new Text();
        HashMap<String, Integer> map =  new HashMap<String, Integer>();
      
        public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {

            Integer maxValue = 0;          
            for (Text val : values)
            {
                String[] parts = val.toString().split("@");
                String bi = parts[0];
            String t = parts[1];
       
        if(t.indexOf(".")==-1){      
                Integer time = Integer.parseInt(t);       
                map.put(bi, time);}

      
                /*Integer time = Integer.parseInt(t);
       
                map.put(bi, time);*/              
            }
            Map.Entry<String,Integer> maxEntry =  getMaxEntry(map);
        String bi = maxEntry.getKey();                 
            bigram.set(bi);
        map.clear();
            context.write(key, bigram);          
        }
    }


    public static void main(String[] args) throws Exception {

    //Leer el archivo resultado y pasarselo al map

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "maxbigram");
        job.setJarByClass(MaxTimesBigramAPartII.class);

        job.setMapperClass(MaxTimesBigramAMapper.class);
        //job.setCombinerClass(MaxTimesUnigramACombiner.class);
        job.setReducerClass(MaxTimesBigramAReducer.class);

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

    public static Entry<String, String> getUnigramEntry(HashMap<String, String> mapUnigram, String decade){     
        Entry<String, String> unigramEntry = null;
        //Integer max = Collections.max(map.values()); 
        for(Entry<String, String> entry : mapUnigram.entrySet()) {
            String key = entry.getKey();
            if(decade.equals(key)) {
                unigramEntry = entry;
            }
        }
        return unigramEntry;
    }

}


  
  
  

   
   
   
	
	
	
