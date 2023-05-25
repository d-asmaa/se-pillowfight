package com.couchbaseFight;

import java.time.Duration;

import org.apache.commons.cli.*;

import com.couchbase.client.core.env.LoggingMeterConfig;
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
        int buffer = 100;

        /* Read request */
        int numReadRequest = 100000;
        String prefixKey = "cb_";
        int startKeyRange = 0; // Can be used for both read and write
        int endKeyRange = 0;

        /* Write request */
        int numWriteRequest = 0;


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
        Option option_pr = Option.builder("pr").argName("prefix_key").hasArg().desc("The prefix of keys").build();

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
                startKeyRange = Integer.parseInt(commandLine.getOptionValue("sk"));
            }
            if (commandLine.hasOption("pr"))
            {
                System.out.printf("Prefix of keys: %s%n", commandLine.getOptionValue("pr"));
                startKeyRange = Integer.parseInt(commandLine.getOptionValue("pr"));
            }
            if (commandLine.hasOption("ek"))
            {
                System.out.printf("last element of key list existing in the cluster to test Get: %s%n", commandLine.getOptionValue("ek"));
                endKeyRange = Integer.parseInt(commandLine.getOptionValue("ek"));
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
            if (commandLine.hasOption("f"))
            {
                System.out.printf("buffer: %s%n", commandLine.getOptionValue("f"));
                buffer = Integer.parseInt(commandLine.getOptionValue("f"));
            }
        }
        catch (ParseException exception)
        {
            System.out.print("Parse error: ");
            System.out.println(exception.getMessage());
        }


        int numConcurrentRequests = 12;

    // TEST METRICS // 

        ClusterEnvironment environment = ClusterEnvironment
        .builder()
        .loggingMeterConfig(LoggingMeterConfig.enabled(false))
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
            cbKV.pushReadRequests(numReadRequest, numConcurrentRequests, startKeyRange, endKeyRange, prefixKey);
        }

        if (numWriteRequest > 0) {
            cbKV.pushWriteRequests(numWriteRequest, numConcurrentRequests, prefixKey);
        }
  
    }}

}
