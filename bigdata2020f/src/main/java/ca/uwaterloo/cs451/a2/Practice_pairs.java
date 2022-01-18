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

package ca.uwaterloo.cs451.a2;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.*;
import java.lang.*;
import java.io.*;

/**
 * Simple word count demo.
 */
public class Practice_pairs extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(Practice_pairs.class);

  // Mapper: emits (token, 1) for every word occurrence.
  public static final class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // Reuse objects to save overhead of object creation.
    public enum Mycounter { LINE_COUNTER };
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      for (String word : Tokenizer.tokenize(value.toString())) {
        WORD.set(word);
        context.write(WORD, ONE);
	Counter counter=context.getCounter(Mycounter.LINE_COUNTER);
	counter.increment(1L);
      }
    }
  }

  

  // Reducer: sums up all the counts.
  public static final class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    // Reuse objects.
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }
  
  
  //second mapper
  
  public static final class MyMapper2 extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
    // Reuse objects to save overhead of object creation.
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        Set<String> set=new HashSet<String>();
      for (String word : Tokenizer.tokenize(value.toString())) {
      set.add(word);
        //WORD.set(word);
        //context.write(WORD, ONE);
      }
      String words[]=new String[set.size()];
      words=set.toArray(words);       // try using words=set.toArray(new String[0]);
      for(int i=0;i<words.length;i++)
      {
      for(int j=i+1;j<words.length;j++)
      {
      PairOfStrings p=new PairOfStrings();
      p.set(words[i],words[j]);
      context.write(p,ONE);
      context.write(new PairOfStrings(words[j],words[i]),ONE);
      }
      }
      
    }
  }
  
  
  
  

  // Reducer: sums up all the counts.
  public static final class MyReducer2 extends Reducer<PairOfStrings, IntWritable, PairOfStrings, PairOfFloatInt> {
    // Reuse objects.
    private static long value;
    private static final IntWritable SUM = new IntWritable();
    Map<String,Integer> mymap= new HashMap<String,Integer>();
    
    @Override
    public void setup(Context context) throws IOException,InterruptedException {
        Configuration conf = context.getConfiguration();
	value=conf.getLong("counter",0L);    
        //strProp = conf.get("my.dijkstra.parameter");
        // and then you can use it
        Path pt=new Path("temp/part-r-00000");
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        try {
          String lines=br.readLine();
          while (lines != null){
          String line[]=lines.split("\\s+");
          if(line.length==2)
          {
          mymap.put(line[0],Integer.parseInt(line[1]));
          }
          //System.out.println(line);

            // be sure to read the next line otherwise you'll get an infinite loop
            lines = br.readLine();
          }
        }
        finally 
        {
          // you should close out the BufferedReader
          br.close();
        }

            }
    
    
    

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      
      //SUM.set(sum);
      //context.write(key, SUM);
      int left=mymap.get(key.getLeftElement());
      int right=mymap.get(key.getRightElement());
      long num_lines=value;
      float pmi=(float)Math.log10((double)(sum*num_lines)/(left*right));
      context.write(key,new PairOfFloatInt(pmi,sum));
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private Practice_pairs() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-imc", usage = "use in-mapper combining")
    boolean imc = false;
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

    LOG.info("Tool: " + Practice_pairs.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);
    LOG.info(" - use in-mapper combining: " + args.imc);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(Practice_pairs.class.getSimpleName());
    job.setJarByClass(Practice_pairs.class);
    String temp_path="temp/";

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(temp_path));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path tempDir = new Path(temp_path);
    FileSystem.get(conf).delete(tempDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    
    
    //job 2
    long count = job.getCounters().findCounter(MyMapper.Mycounter.LINE_COUNTER).getValue();
    conf.setLong("counter", count);
    Job job2 = Job.getInstance(conf);
    job2.setJobName(Practice_pairs.class.getSimpleName());
    job2.setJarByClass(Practice_pairs.class);

    job2.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job2, new Path(args.input));
    FileOutputFormat.setOutputPath(job2, new Path(args.output));

    job2.setMapOutputKeyClass(PairOfStrings.class);
    job2.setMapOutputValueClass(IntWritable.class);
    job2.setOutputKeyClass(PairOfStrings.class);
    job2.setOutputValueClass(PairOfFloatInt.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    job2.setMapperClass(MyMapper2.class);
    //job2.setCombinerClass(MyReducer2.class);
    job2.setReducerClass(MyReducer2.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime1 = System.currentTimeMillis();
    job2.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime1) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Practice_pairs(), args);
  }
}
