package com.couchbaseFight;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.GetResult;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class CouchbaseKV {
    ReactiveCollection collection;
    // AtomicReference<Long> counter = new AtomicReference<Long>();
    // AtomicReference<Long> accumuCounter = new AtomicReference<Long>();
    int counter = 0;
    int accumuCounter = 0;

    Instant startTime;
    public CouchbaseKV(ReactiveCollection collection) {
        this.collection = collection;
    }
    
    public void pushReadRequests(int numOperations, int numTasks, int numThreads, int start, int end, String prefix){
        // counter.set(0L); // Counter for number of operations
        // accumuCounter.set(0L);
        counter = 0;
        accumuCounter = 0;
        startTime = Instant.now(); // Start time for measuring duration

        Flux.range(0, numTasks)
        .parallel()
       // .runOn(Schedulers.newParallel("asmaa-parallel", 12))
        .runOn(Schedulers.newParallel("asmaa-parallel", numThreads))
        .flatMap(i -> Flux.range(0, numOperations)
                .flatMap(j -> {
                    //String documentId= prefix + ThreadLocalRandom.current().nextInt(start, end + 1);
                    // if (accumuCounter.get() > numOperations) {
                    //     System.exit(0);
                    // }
                    String documentId = "00002c88-0260-4d11-ab63-d21c98b38874";
                    return Flux.just(documentId)
                            .flatMap(id -> (this.collection.get(id))
                                    .map(GetResult::contentAsObject)
                            );
                })
        ).doOnNext(result -> {
            // accumuCounter.accumulateAndGet(1L, Long::sum);
            // long cnt = counter.accumulateAndGet(1L, Long::sum);
            accumuCounter ++;
            counter ++ ;
           // if (cnt % 10000L == 0) {
            if (counter % 10000 == 0) {
                Instant endTime = Instant.now();
                long durationMillis = Duration.between(startTime, endTime).toMillis();
                double opsPerSecond =  (counter / durationMillis) * 1000.0;
                DecimalFormat f = new DecimalFormat("##.00");
                System.out.println(" Read Operations per second: " + f.format(opsPerSecond));
                startTime = Instant.now(); // Reset start time for the next batch
                //counter.set(0L);
                counter = 0;
            }
        })
        .sequential()
        .blockLast();


    }

    public void pushWriteRequests(int numOperations, int numTasks, int numThreads, String prefix){
        counter = 0;
        accumuCounter = 0;
        startTime = Instant.now(); // Start time for measuring duration

        DocGenerator docGenerator = new DLDocGenerator();
        Flux.range(0, numTasks)
        .parallel()
        .runOn(Schedulers.newParallel("asmaa-parallel", numThreads)) 
        .flatMap(i -> Flux.range(0, numOperations)
                .flatMap(j -> { 
                   String documentId = UUID.randomUUID().toString();
                    return collection.upsert(documentId, docGenerator.generateDoc(i));
                })
        ).doOnNext(result -> {
            accumuCounter ++;
            counter ++ ;
            if ( counter % 10000L == 0) {
                Instant endTime = Instant.now();
                long durationMillis = Duration.between(startTime, endTime).toMillis();
                double opsPerSecond = (double) counter / (double) durationMillis * 1000.0;
                DecimalFormat f = new DecimalFormat("##.00");
                System.out.println("Write Operations per second: " + f.format(opsPerSecond));
                startTime = Instant.now(); // Reset start time for the next batch
                counter = 0;
            }
        })
        .sequential()
        .blockLast();
    }

    private static String generateKey(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(value);

        byte[] bytes = buffer.array();

        StringBuilder keyBuilder = new StringBuilder();

        for (byte b : bytes) {
            keyBuilder.append(String.format("%02x", b));
        }

        return keyBuilder.toString();
    }
}
