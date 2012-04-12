import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.RowCounter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

public class LetterCountJob extends Configured implements Tool {
    public static final String NAME = "LetterCount";
    public enum Counters { ROWS, ERROR, VALID }

    static class AnalyzeMapper extends TableMapper<Text, IntWritable> {
        private IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(ImmutableBytesWritable key, Result cols, Context context) throws IOException {
            context.getCounter(Counters.ROWS).increment(1);

            try {
                String sKey = Bytes.toString(key.get());
                if(context.getConfiguration().get("conf.debug") != null)
                    System.out.println(sKey);

                context.write(new Text(sKey.substring(0, 1)), ONE);
                context.getCounter(Counters.VALID).increment(1);
            } catch(Exception e) {
                e.printStackTrace();
                context.getCounter(Counters.ERROR).increment(1);
            }
        }
    }

    static class AnalyzeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // jetzt bekommen wir stets eintraege zu einem key, d.h. wir zaehlen values
            int count = 0;

            for(IntWritable val: values)
                count += val.get();

            context.write(key, new IntWritable(count));
        }
    }

    private static CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();
        Option o = new Option("t", "table", true,
                "table to read from (must exist)");
        o.setArgName("table-name");
        o.setRequired(true);
        options.addOption(o);
        o = new Option("o", "output", true,
                "the directory to write to");
        o.setArgName("path-in-HDFS");
        o.setRequired(true);
        options.addOption(o);
        options.addOption("d", "debug", false, "switch on DEBUG log level");
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage() + "\n");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(NAME + " ", options, true);
            System.exit(-1);
        }
        if (cmd.hasOption("d")) {
            Logger log = Logger.getLogger("mapreduce");
            log.setLevel(Level.DEBUG);
            System.out.println("DEBUG ON");
        }
        return cmd;
    }

    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        String[] otherArgs =
                new GenericOptionsParser(conf, args).getRemainingArgs();
        CommandLine cmd = parseArgs(otherArgs);
        // check debug flag and other options
        if (cmd.hasOption("d")) conf.set("conf.debug", "true");
        // get details
        String table = cmd.getOptionValue("t");
        String output = cmd.getOptionValue("o");

        // Scan interface defines "what we get" from HBase
        Scan scan = new Scan();

        Job job = new Job(conf, "Analyze data in " + table);
        job.setJarByClass(LetterCountJob.class);
        TableMapReduceUtil.initTableMapperJob(table, scan, AnalyzeMapper.class,
                Text.class, IntWritable.class, job);
        job.setReducerClass(AnalyzeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(output));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new LetterCountJob(), args);
        System.exit(ret);
    }
}
