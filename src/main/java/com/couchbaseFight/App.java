package com.couchbaseFight;

import java.time.Duration;

import org.apache.commons.cli.*;

import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.java.*;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.env.ClusterEnvironment;


public class App 
{
    public static void main( String[] args )
    {
        String username = "cbuser";
        String password = "password";
        String ip = "127.0.0.1";
        String bucketName = "appointment";
        String scopeName = "_default";
        String collectionName = "_default";

        /* Read request */
        int numReadRequest = 100000;
        String prefixKey = "cb";
        String separator = "_";
        Long startKeyRange = 0L; // Can be used for both read and write
        Long endKeyRange = 49999L;

        /* Write request */
        int numWriteRequest = 0;

        int numTasks = 2;
        int numThreads = 12;
        Long logAfter = 100L;
        int speed = 0;
        


        CommandLine commandLine;
        Option option_h = Option.builder("h").argName("host").hasArg().desc("couchbase ip").build();
        Option option_u = Option.builder("u").argName("username").hasArg().desc("couchbase username").build();
        Option option_p = Option.builder("p").argName("password").hasArg().desc("couchbase password").build();
        Option option_b = Option.builder("b").argName("bucket").hasArg().desc("couchbase bucket").build();
        Option option_f = Option.builder("f").argName("buffer").hasArg().desc("buffer").build();
        Option option_s = Option.builder("s").argName("scope").hasArg().desc("couchbase scope").build();
        Option option_c = Option.builder("c").argName("collection").hasArg().desc("couchbase collection").build();
        Option option_nr = Option.builder("nr").argName("num_read").hasArg().desc("number of read requests to execute").build();
        Option option_nw = Option.builder("nw").argName("num_write").hasArg().desc("number of write requests to execute").build();
        Option option_sk = Option.builder("sk").argName("start_key_range").hasArg().desc("the first element of the key range to execute the get on it").build();
        Option option_ek = Option.builder("ek").argName("end_key_range").hasArg().desc("the last element of the key range to execute the get on it").build();
        Option option_pr = Option.builder("pr").argName("prefix_key").hasArg().desc("The prefix of keys. By default: cb").build();
        Option option_tasks = Option.builder("tpt").argName("tasks_per_thread").hasArg().desc("number of tasks per thread. recommended: 1 or 2").build();
        Option option_thread = Option.builder("th").argName("num_threads").hasArg().desc("number of threads").build();
        Option option_separator = Option.builder("sep").argName("separator").hasArg().desc("separator between prefix and document key. by default : _ ").build();
        Option option_log = Option.builder("lg").argName("log-after").hasArg().desc("log metrics after x requests. by default : 10000 ").build();
        Option option_speed = Option.builder("sp").argName("speed").hasArg().desc("speed(req/sec)").build();

        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption(option_h);
        options.addOption(option_u);
        options.addOption(option_p);
        options.addOption(option_nr);
        options.addOption(option_nw);
        options.addOption(option_sk);
        options.addOption(option_ek);
        options.addOption(option_b);
        options.addOption(option_c);
        options.addOption(option_s);
        options.addOption(option_f);
        options.addOption(option_pr);
        options.addOption(option_tasks);
        options.addOption(option_thread);
        options.addOption(option_separator);
        options.addOption(option_log);
        options.addOption(option_speed);

        String header = "      [<arg1> [<arg2> [<arg3> ...\n       Options, flags and arguments may be in any order";
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("CLIsample", header, options, null, true);


        try
        {
            commandLine = parser.parse(options, args);

            if (commandLine.hasOption("h"))
            {
                System.out.printf("host ip: %s%n", commandLine.getOptionValue("h"));
                ip = commandLine.getOptionValue("h");
            }

            if (commandLine.hasOption("u"))
            {
                System.out.printf("couchbase username: %s%n", commandLine.getOptionValue("u"));
                username = commandLine.getOptionValue("u");
            }

            if (commandLine.hasOption("p"))
            {
                System.out.printf("couchbase password: %s%n", commandLine.getOptionValue("p"));
                password = commandLine.getOptionValue("p");
            }

            if (commandLine.hasOption("nr"))
            {
                System.out.printf("number of read requests to run: %s%n", commandLine.getOptionValue("nr"));
                numReadRequest = Integer.parseInt(commandLine.getOptionValue("nr"));
            }
            if (commandLine.hasOption("nw"))
            {
                System.out.printf("number of write requests to run: %s%n", commandLine.getOptionValue("nw"));
                numWriteRequest = Integer.parseInt(commandLine.getOptionValue("nw"));
            }
            if (commandLine.hasOption("sk"))
            {
                System.out.printf("first element of key list existing in the cluster to test Get: %s%n", commandLine.getOptionValue("sk"));
                startKeyRange = Long.parseLong(commandLine.getOptionValue("sk"));
            }
            
            if (commandLine.hasOption("ek"))
            {
                System.out.printf("last element of key list existing in the cluster to test Get function: %s%n", commandLine.getOptionValue("ek"));
                endKeyRange = Long.parseLong(commandLine.getOptionValue("ek"));
            }
            if (commandLine.hasOption("b"))
            {
                System.out.printf("couchbase bucket: %s%n", commandLine.getOptionValue("b"));
                bucketName = commandLine.getOptionValue("b");
            }
            if (commandLine.hasOption("s"))
            {
                System.out.printf("couchbase scope: %s%n", commandLine.getOptionValue("s"));
                scopeName = commandLine.getOptionValue("s");
            }
            if (commandLine.hasOption("c"))
            {
                System.out.printf("couchbase collection: %s%n", commandLine.getOptionValue("c"));
                collectionName = commandLine.getOptionValue("c");
            }
            if (commandLine.hasOption("tpt"))
            {
                System.out.printf("number of tasks per thread: %s%n", commandLine.getOptionValue("tpt"));
                numTasks = Integer.parseInt(commandLine.getOptionValue("tpt"));
            }
            if (commandLine.hasOption("th"))
            {
                System.out.printf("number of threads: %s%n", commandLine.getOptionValue("th"));
                numThreads = Integer.parseInt(commandLine.getOptionValue("th"));
            }
            if (commandLine.hasOption("pr"))
            {
                System.out.printf("Prefix of keys: %s%n", commandLine.getOptionValue("pr"));
                prefixKey = commandLine.getOptionValue("pr");
            }
            if (commandLine.hasOption("sep"))
            {
                System.out.printf("Separator between prefix and keys : %s%n", commandLine.getOptionValue("sep"));
                separator = commandLine.getOptionValue("sep");
            }
            if (commandLine.hasOption("lg"))
            {
                System.out.printf("log metrics after %s requests %n ", commandLine.getOptionValue("lg"));
                logAfter = Long.parseLong(commandLine.getOptionValue("lg"));
            }
            if (commandLine.hasOption("sp"))
            {
                System.out.printf("speed: %s  req/sec %n ", commandLine.getOptionValue("sp"));
                speed = Integer.parseInt(commandLine.getOptionValue("sp"));
            }
        }
        catch (ParseException exception)
        {
            System.out.print("Parse error: ");
            System.out.println(exception.getMessage());
        }

        ClusterEnvironment environment = ClusterEnvironment
        .builder()
        .retryStrategy(BestEffortRetryStrategy.withExponentialBackoff(Duration.ofNanos(1000),
                                Duration.ofMillis(1), 2))
        .build();

        try(
            Cluster cluster = Cluster.connect(
                    ip,
                    ClusterOptions.clusterOptions(username, password).environment(environment)
                    )
            ) {

        ReactiveBucket bucket = cluster.bucket(bucketName).reactive();
        ReactiveScope scope = bucket.scope(scopeName);
        ReactiveCollection collection = scope.collection(collectionName);

        CouchbaseKV cbKV = new CouchbaseKV(collection);
        /* Read */
        if (numReadRequest > 0) {
            cbKV.pushReadRequests(numReadRequest/numTasks, numTasks, numThreads,  startKeyRange, endKeyRange, prefixKey+separator, logAfter , speed);
            cbKV.pushReadRequests(numReadRequest%numTasks, 1, 1, startKeyRange, endKeyRange, prefixKey+separator, logAfter, speed);
        }

        if (numWriteRequest > 0) {
            cbKV.pushWriteRequests(numWriteRequest/numTasks, numTasks, numThreads, startKeyRange, prefixKey+separator, logAfter, speed);
            cbKV.pushWriteRequests(numWriteRequest%numTasks, 1, 1, startKeyRange + numWriteRequest/numTasks +1, prefixKey+separator, logAfter, speed);
        }
  
    }}

}
