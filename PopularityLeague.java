import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }
    
    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    @Override
    public int run(String[] args) throws Exception {
    	  Job job = Job.getInstance(this.getConf(), "Popularity League");
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(IntWritable.class);

          job.setMapOutputKeyClass(Text.class);
          job.setMapOutputValueClass(IntWritable.class);

          job.setMapperClass(LinkCountMap.class);
          job.setReducerClass(LinkRankReduce.class);

          FileInputFormat.setInputPaths(job, new Path(args[0]));
          FileOutputFormat.setOutputPath(job, new Path(args[1]));

          job.setJarByClass(PopularityLeague.class);
          return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public static class LinkCountMap extends Mapper<Object, Text, Text, IntWritable> {
        List<String> leagueMembers;
        
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String leagueMembersPath = conf.get("league");
            this.leagueMembers = Arrays.asList(readHDFSFile(leagueMembersPath, conf).split("\n"));
        }

            
            @Override public void map(Object key, Text value, Context context)  throws IOException, InterruptedException {
               StringTokenizer t  = new StringTokenizer(value.toString(), ": ");
               Text page = new Text(t.nextToken());
               //if (leagueMembers.contains(page)) context.write(page, new IntWritable(0));
               while (t.hasMoreTokens()) {
                 Text link = new Text(t.nextToken());
                 if (leagueMembers.contains(link.toString())) context.write(link, new IntWritable(1));
               }
            }
      }

      public static class LinkRankReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    	  List<String> leagueMembers;
    	  TreeSet<Pair<Integer, String>> rankings;
          
    	  @Override
          protected void setup(Context context) throws IOException,InterruptedException {
              Configuration conf = context.getConfiguration();
              String leagueMembersPath = conf.get("league");
              this.leagueMembers = Arrays.asList(readHDFSFile(leagueMembersPath, conf).split("\n"));
          }
    	  
          @Override public void reduce(Text key, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException {
        	  String page = key.toString();
        	  if (leagueMembers.contains(page)) {
	            int sum = 0; 
	            for (IntWritable value : values) {
	               sum += value.get();
	            }
	            rankings.add(Pair.of(sum, page));
        	}
          }

		@Override
		protected void cleanup(Context context)	throws IOException, InterruptedException {
			int rank = 0; int lastCount = -1;
			for (Pair<Integer, String> page : rankings) {
				context.write(new Text(page.second), new IntWritable(page.first));
				rank++;
			}
		}
          
          

      }
}