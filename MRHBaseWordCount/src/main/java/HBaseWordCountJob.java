import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

/*
HBase Word Count Beispiel:
- Hier wird aus HBase gelesen und in HBase geschrieben
- Wesentlich beim Einsatz von HBase als Quelle ist die Definition einer Scan-Instanz (siehe run()), diese
  definiert im Prinzip "was" in den Map-Vorgang hineinkommt
- Damit der Reducer funktioniert, muss die Zieltabelle schon existieren.
 */


public class HBaseWordCountJob extends Configured implements Tool {
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


    // public abstract class TableReducer<KEYIN, VALUEIN, KEYOUT>
    static class AnalyzeReducer extends TableReducer<Text, IntWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // jetzt bekommen wir stets eintraege zu einem key, d.h. wir zaehlen values
            int count = 0;

            for(IntWritable val: values)
                count += val.get();

            // HBase output ist analog zu Commando ein Put Objekt:
            Put put = new Put(key.getBytes());

            // schreibe count in column data:count
            put.add(Bytes.toBytes("data"), Bytes.toBytes("count"), Bytes.toBytes(count));

            context.write(key, put);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        String[] otherArgs =
                new GenericOptionsParser(conf, args).getRemainingArgs();
        CommandLine cmd = parseArgs(otherArgs);
        // check debug flag and other options
        if (cmd.hasOption("d")) conf.set("conf.debug", "true");
        // get details
        String tableIn = cmd.getOptionValue("tIn");
        String tableOut = cmd.getOptionValue("tOut");

        // Scan interface defines "what we get" from HBase, here: everything!
        Scan scan = new Scan();

        Job job = new Job(conf, "Analyze data in " + tableIn);
        job.setJarByClass(HBaseWordCountJob.class);
        TableMapReduceUtil.initTableMapperJob(tableIn, scan, AnalyzeMapper.class,
                Text.class, IntWritable.class, job);

        TableMapReduceUtil.initTableReducerJob(tableOut,
                AnalyzeReducer.class, job);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new HBaseWordCountJob(), args);
        System.exit(ret);
    }

    private static CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();

        // input table
        Option o = new Option("tIn", "inputTable", true,
                "table to read from (must exist)");
        o.setArgName("table-name");
        o.setRequired(true);
        options.addOption(o);

        // output table
        o = new Option("tOut", "outputTable", true,
                "table to read from (must exist)");
        o.setArgName("table-name");
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

}
