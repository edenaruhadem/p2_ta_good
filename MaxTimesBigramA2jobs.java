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

public class MaxTimesBigramA2jobs
{

    public static class MaxTimesBigramAMapperjob1 extends Mapper<Object, Text, Text, Text>
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


    public static class MaxTimesBigramAReducerjob1 extends Reducer<Text, Text, Text, Text>
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


    public static class MaxTimesBigramAMapperjob2 extends Mapper<Object, Text, Text, Text>
    {
        private Text year = new Text();      
        private Text bitimes = new Text();
        HashMap<String, String> mapUnigram =  new HashMap<String, String>();
        //Path unigramFile = new Path("/output/part-r-00000");
        //Aqui leer archivo y rellenar
   
    public void setup(Context context) throws IOException{
        Path pt=new Path("/OutputPart11/part-r-00000");//Location of file in HDFS
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        line=br.readLine();
    String [] parts = line.split("\t");
        String dec = parts[0];
        String unigram = parts[1];
        //System.out.println(dec+unigram);
        mapUnigram.put(dec,unigram);
        do{
            //System.out.println(line);
            line=br.readLine();
        //System.out.println(line);
        if(line!=null){
            parts = line.split("\t");
            dec = parts[0];
            unigram = parts[1];
            mapUnigram.put(dec,unigram);}
        }while(line!=null);
    br.close();
        }


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
          //System.out.println(value.toString());     
        String [] partsLine = value.toString().trim().split("\t");
        String bigram = partsLine[0];      
        String stryear = partsLine[1];
        String times = partsLine[2];
        //System.out.println(bigram+"/"+stryear+"/"+times);
        Integer y = Integer.parseInt(stryear);
        if(y>=1900)
        {
        Integer lastnumy = Integer.parseInt(stryear.substring(stryear.length()-1));
    Integer decade = y - lastnumy;
        //Integer decade = y + (10-lastnumy);
        String strdecade = Integer.toString(decade);
        Map.Entry<String,String> unigramEntry = getUnigramEntry(mapUnigram, strdecade);
    //System.out.println(unigramEntry.getValue()+"/"+unigramEntry.getKey());
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
    else if(sizeUni == 1)
        {
        String oneuni = unigramEntry.getValue();
        //System.out.println(oneuni+"biiiiiiigrammmm"+bigram);
        String [] splitBigram = bigram.split(" ");
        String uni = splitBigram[0];
        if(uni.equals(oneuni)){
            String wordBigram = splitBigram[1];
            //System.out.println(oneuni+"biiiiigram: "+uni+" "+wordBigram);
            year.set(strdecade);
                    bitimes.set(wordBigram+"@"+times);
                    context.write(new Text(year), new Text(bitimes));
        }       
    }       
        }                        
        }
    }

    public static class MaxTimesBigramAReducerjob2 extends Reducer<Text, Text, Text, Text>
    {
      
        private Text bigram = new Text();
        HashMap<String, Integer> map =  new HashMap<String, Integer>();
      
        public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {

            Integer maxValue = 0;          
            for (Text val : values)
            {
        //System.out.println(val.toString());   //wordbigram@times
                String[] parts = val.toString().trim().split("@");
                String bi = parts[0];
            String t = parts[1];
        //System.out.println(bi+t);
        if((t.indexOf(".")==-1) && (!t.equals("_NUM")) && (!t.equals("")) && (!t.equals("_NOUN"))){      
                Integer time = Integer.parseInt(t);
        //System.out.println("word: "+bi+" time: "+t);       
                map.put(bi, time);
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


    public static void main(String[] args) throws Exception {

    //Leer el archivo resultado y pasarselo al map

        Configuration conf = new Configuration();
        Path interpath = new Path("/OutputPart11");

        Job job = Job.getInstance(conf, "maxbigramjob1");	    
	    job.setJarByClass(MaxTimesBigramA2jobs.class);	    
	    job.setMapperClass(MaxTimesBigramAMapperjob1.class);
	    job.setReducerClass(MaxTimesBigramAReducerjob1.class);	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);	    
	    FileInputFormat.addInputPath(job,new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, interpath);	    
	    job.waitForCompletion(true);


        Job job1 = Job.getInstance(conf, "maxbigramjob2");
        job1.setJarByClass(MaxTimesBigramA2jobs.class);
        job1.setMapperClass(MaxTimesBigramAMapperjob2.class);        
        job1.setReducerClass(MaxTimesBigramAReducerjob2.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
      
    System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
 

    public static Entry<String, Integer> getMaxEntry(HashMap<String, Integer> map){     
        Entry<String, Integer> maxEntry = null;
        Integer max = Collections.max(map.values());
    //System.out.format("El maximo es: %d",max);
    //System.out.println(" @@@@@@@"); 
        for(Entry<String, Integer> entry : map.entrySet()) {
            Integer value = entry.getValue();
        //String key = entry.getKey();
        //System.out.print("key: "+key);
        //System.out.format(" value: %d",value);
            if(null != value && max == value) {
                maxEntry = entry;
        //System.out.print("key: "+maxEntry.getKey());
            //System.out.format(" value: %d",maxEntry.getValue());
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