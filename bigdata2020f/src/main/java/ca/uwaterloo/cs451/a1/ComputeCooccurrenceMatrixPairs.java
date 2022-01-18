/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs451.a1;

import org.apache.hadoop.conf.Configuration;
import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.*;
import tl.lin.data.map.HashMapWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.*;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.*;
/**
 * <p>
 * Implementation of the "pairs" algorithm for computing co-occurrence matrices from a large text
 * collection. This algorithm is described in Chapter 3 of "Data-Intensive Text Processing with
 * MapReduce" by Lin &amp; Dyer, as well as the following paper:
 * </p>
 *
 * <blockquote>Jimmy Lin. <b>Scalable Language Processing Algorithms for the Masses: A Case Study in
 * Computing Word Co-occurrence Matrices with MapReduce.</b> <i>Proceedings of the 2008 Conference
 * on Empirical Methods in Natural Language Processing (EMNLP 2008)</i>, pages 419-428.</blockquote>
 *
 * @author Jimmy Lin
 */




public class ComputeCooccurrenceMatrixPairs extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ComputeCooccurrenceMatrixPairs.class);
  private static final HashMap <String,Integer>  hh =new HashMap<>();
  private static final HashMapWritable<PairOfStrings,IntWritable> hp=new HashMapWritable<>();
  private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
    private static final PairOfStrings PAIR = new PairOfStrings();
    private static final IntWritable ONE = new IntWritable(1);
    private int window = 2;



    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(value.toString());

      for (int i = 0; i < tokens.size(); i++) {
        for (int j = Math.max(i - window, 0); j < Math.min(i + window + 1, tokens.size()); j++) {
            String a=tokens.get(i);
            String b=tokens.get(j);
          if (i == j) continue;
            if(hh.containsKey(a))
                {
                    hh.put(a,hh.get(a)+1);
                }
             else if(hh.containsKey(b))
             {
                 hh.put(b,hh.get(b)+1);
             }

             else
             {
                 if(! hh.containsKey(a))
                     hh.put(a,1);
                 else
                     hh.put(b,1);
             }


          PAIR.set(a, b);
          context.write(PAIR, ONE);
        }
      }
    }
  }

  private static final class MyReducer extends
      Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      SUM.set(sum);
          //  for (Map.Entry mapElement : hh.entrySet()) {
           // String key1 = (String)mapElement.getKey();

            // Add some bonus marks
            // to all the students and print it
            //int value = ((int)mapElement.getValue() + 10);

            //System.out.println(key1 + " : " + value);
            //}
      // hp.put(key,sum);
      context.write(key, SUM);

    }
  }

  private static final class MyPartitioner extends Partitioner<PairOfStrings, IntWritable> {
    @Override
    public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }







    //second stage mapper


    private static class PairsPMIMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {

    // Objects for reuse
    private final static PairOfStrings PAIR = new PairOfStrings();
    private final static IntWritable ONE = new IntWritable(1);
        private int window =2;
 //      File file = new File("cs451-bigdatateach-inter/part-r-00000");
//      BufferedReader br = new BufferedReader(new FileReader(file));
    @Override
    public void setup(Context context) {
      window = context.getConfiguration().getInt("window", 2);

       // File file = new File("cs451-bigdatateach-inter/part-r-00000");
//      try
//      {
        //  BufferedReader br = new BufferedReader(new FileReader(file));

  //    String st;
      //while ((st = br.readLine()) != null)
//        System.out.println(st);

//        }
//      catch (IOException ioe)
  //     {
//         ioe.printStackTrace();
  //     }
        
           File file = new File("cs451-bigdatateach-inter/part-r-00000");
        try{
        BufferedReader br = new BufferedReader(new FileReader(file));

      String st;
      while ((st = br.readLine()) != null) {
//
        String[] ans=st.split("\\s+");
        //for(String a:ans){System.out.println(a);
//      System.out.println(ans.length);
        PairOfStrings ps=new PairOfStrings();
//      String[] s=ans[0].split(",");
//      for(String si:ans){System.out.println(si+"Here");}
//      System.out.println(ans[0]+ans[1]);
        int total=Integer.parseInt(ans[2]);
        String ty=ans[0]+ans[1];
        String[] ate=ty.split(",");
        String l=ate[0];
        String r=ate[1];
          ps.set(l,r);
          hp.put(ps,new IntWritable(total));
          
        
        
        
    }
        }
        catch (IOException ioe) 
       {
	   ioe.printStackTrace();
       } 
    }
        
        

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{
        PairOfStrings nn=new PairOfStrings("Hello","World");
        hp.put(nn,new IntWritable(3));
        System.out.println(hp.get(nn));
     // String line = value.toString();
      //StringTokenizer t = new StringTokenizer(line);
       IntWritable ONE=new IntWritable(1);
       PairOfWritables pp=new PairOfWritables<IntWritable,IntWritable>();
          List<String> tokens = Tokenizer.tokenize(value.toString());
	for(String a:tokens){System.out.println(a+" ");}
            //System.out.println(tokens.size()+"size         ");
                //System.out.println(value);
      
//          String l=s[0].substring(1);
  //        String r=s[1].substring(0,s[1].length()-1);
//             String l=tokens.get(0);
             PairOfStrings ps=new PairOfStrings();
//            String r=tokens.get(1);
	for (int i = 0; i < tokens.size(); i++) {
        for (int j = Math.max(i - window, 0); j < Math.min(i + window + 1, tokens.size()); j++) {
            String a=tokens.get(i);
            String b=tokens.get(j);
          if (i == j) continue;
          //  if(hh.containsKey(a))
         //       {
          //          hh.put(a,hh.get(a)+1);
        //        }
           //  else if(hh.containsKey(b))
         //    {
       //          hh.put(b,hh.get(b)+1);
             //}

          //   else
            // {
               //  if(! hh.containsKey(a))
             //        hh.put(a,1);
           //      else
         //            hh.put(b,1);
       //      }


          PAIR.set(a, b);
          context.write(PAIR, ONE);
        }
      }
       // ps.set(l,r);
    //   int aa =Integer.parseInt(ans[1]);
        context.write(ps,new IntWritable(1));
        //br.close();
        //break;
}
        //}
    }    
     
//       String l=tokens.get(0);
           //  PairOfStrings ps=new PairOfStrings();
    //        String r=tokens.get(1);
            //int ii=Integer.parseInt(tokens.get(2));
            //System.out.println(l+r+"xxxxxxxxxxxxxxxxx");
            //for(String a:tokens){
              //  System.out.println(a+"Herexxxxxxxxxxxxxxxxxxx");
               //String[] parts = a.split("\\s+");

                //ps.set("manish","kumar");
             //System.out.println(ps+"psddddddddddddddddddddd");
            //}
            // ps.set("Manish","Kumar");
            //pp.set(new IntWritable(1),new IntWritable(1));
                 //IntWritable ii=new IntWritable(Integer.parseInt(parts[2]));
              //  context.write(ps,ONE); // x y x&y in same order
            

     // Set<String> sortedTerms = new TreeSet<String>();
      //while(t.hasMoreTokens()){
        //sortedTerms.add(t.nextToken());
      //}



        





     // Second Stage reducer: Finalizes PMI Calculation given
  private static class PairsPMIReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, PairOfWritables<FloatWritable,IntWritable>> {

    private static Map<String, Integer> termTotals = new HashMap<String, Integer>();

    private static FloatWritable PMI = new FloatWritable();
    private static double totalDocs = 156215.0;


    @Override
    public void reduce(PairOfStrings pair, Iterable<IntWritable> values, Context context )
        throws IOException, InterruptedException{
      // Recieving pair and pair counts -> Sum these for this pair's total
        // Only calculate PMI for pairs that occur 10 or more times
         int pairSum=0;


      for(IntWritable value : values) {
         // String v=Tokenizer.tokenize(value.toString()); //actually i dont need this i already stored it in hashmap i need only pair sum

        pairSum += value.get();
      }

      //if(pairSum >= 10){

        // Look up individual totals for each member of pair
        // Calculate PMI emit Pair or Text as key and Float as value
        String left = pair.getLeftElement();
        String right = pair.getRightElement();



        int x=1;int y=1;
        if(hh.containsKey(left)|| hh.containsKey(right)){
        if(hh.containsKey(left)){x=hh.get(left);}
        if(hh.containsKey(right)){y=hh.get(right);}

        }
        pairSum=hp.get(pair).get();
        double probPair = pairSum / totalDocs;
        double probLeft = x / totalDocs;
        double probRight = y / totalDocs;
        float pmi = (float)Math.log(probPair / (probLeft * probRight));

        pair.set(left, right);
//        System.out.println(pair+" "+pmi+" "+pairSum);
        PMI.set(pmi);
          //PairOfFloatInt pw=new PairOfFloatInt(pmi,pairSum);
          IntWritable st=new IntWritable(pairSum);
        context.write(pair, new PairOfWritables<>(PMI,st));
      }

    }


















  /**
   * Creates an instance of this tool.
   */
  private ComputeCooccurrenceMatrixPairs() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-window", metaVar = "[num]", usage = "cooccurrence window")
    int window = 2;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }
    //String outputPath = cmdline.getOptionValue(OUTPUT);
        String intermediatePath = "cs451-bigdatateach-inter";



        LOG.info("Tool: " + PairsPMI.class.getSimpleName() + " Appearances Part");
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + intermediatePath);
        LOG.info(" - number of reducers: " + args.numReducers);

        Configuration conf = getConf();
        conf.set("intermediatePath", intermediatePath);


    Job job1 = Job.getInstance(getConf());
    job1.setJobName(ComputeCooccurrenceMatrixPairs.class.getSimpleName());
    job1.setJarByClass(ComputeCooccurrenceMatrixPairs.class);

    // Delete the output directory if it exists already.
     FileInputFormat.setInputPaths(job1, new Path(args.input));
        FileOutputFormat.setOutputPath(job1, new Path(intermediatePath));


    //job1.getConfiguration().setInt("window", args.window);

    job1.setNumReduceTasks(args.numReducers);

        Path intermediateDir = new Path(intermediatePath);
        FileSystem.get(conf).delete(intermediateDir, true);


    job1.setMapOutputKeyClass(PairOfStrings.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(PairOfStrings.class);
    job1.setOutputValueClass(IntWritable.class);

    job1.setMapperClass(MyMapper.class);
    job1.setCombinerClass(MyReducer.class);
    job1.setReducerClass(MyReducer.class);
    job1.setPartitionerClass(MyPartitioner.class);





    long startTime = System.currentTimeMillis();
    job1.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");



    // Start second job

        LOG.info("Tool: " + ComputeCooccurrenceMatrixPairs.class.getSimpleName() + " Pairs Part");
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - number of reducers: " + args.numReducers);

        Job job2 = Job.getInstance(getConf());
        job2.setJobName(ComputeCooccurrenceMatrixPairs.class.getSimpleName() + " PairsPMICalcuation");
        job2.setJarByClass(ComputeCooccurrenceMatrixPairs.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job2,  new Path(args.input));
        TextOutputFormat.setOutputPath(job2, new Path(args.output));

        //TODO Which output key??
        job2.setOutputKeyClass(PairOfStrings.class);
        job2.setOutputValueClass(PairOfWritables.class);
        job2.setMapperClass(PairsPMIMapper.class);
        //job2.setCombinerClass(PairsPMICombiner.class);
        job2.setReducerClass(PairsPMIReducer.class);

        Path outputDir = new Path(args.output);
        FileSystem.get(conf).delete(outputDir, true);

        startTime = System.currentTimeMillis();
        job2.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");



    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ComputeCooccurrenceMatrixPairs(), args);
  }
}
