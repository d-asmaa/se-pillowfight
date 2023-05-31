package com.couchbaseFight;

import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.GetResult;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class CouchbaseKV {
    ReactiveCollection collection;
    AtomicReference<Long> counter = new AtomicReference<Long>();
    AtomicReference<Long> accumuCounter = new AtomicReference<Long>();

    Instant startTime;
    public CouchbaseKV(ReactiveCollection collection) {
        this.collection = collection;
    }
    
    public void pushReadRequests(int numOperations, int numTasks, int numThreads, Long start, Long end, String prefix, Long logAfter){
        counter.set(0L); // Counter for number of operations
        accumuCounter.set(0L);
        startTime = Instant.now(); // Start time for measuring duration

        Flux.range(0, numTasks)
        .parallel()
        .runOn(Schedulers.newParallel("se-stresstool", numThreads))
        .flatMap(i -> Flux.range(0, numOperations)
                .flatMap(j -> {
                    String documentId= prefix + new Random().nextInt(0, numTasks) + new Random().nextLong(start, end);
                    return Flux.just(documentId)
                            .flatMap(id -> (this.collection.get(id))
                                    .map(GetResult::contentAsObject)
                            );
                })
        ).doOnNext(result -> {
            accumuCounter.accumulateAndGet(1L, Long::sum);
            long cnt = counter.accumulateAndGet(1L, Long::sum);
            if (cnt % logAfter == 0) {
                Instant endTime = Instant.now();
                long durationMillis = Duration.between(startTime, endTime).toMillis();
                double opsPerSecond =  (cnt / durationMillis) * 1000.0;
                DecimalFormat f = new DecimalFormat("##.00");
                System.out.println(" Read Operations per second: " + f.format(opsPerSecond));
                startTime = Instant.now(); // Reset start time for the next batch
                counter.set(0L);
            }
        })
        .sequential()
        .blockLast();


    }

    public void pushWriteRequests(int numOperations, int numTasks, int numThreads, Long start, String prefix, Long logAfter){
        counter.set(0L); // Counter for number of operations
        accumuCounter.set(0L);
        startTime = Instant.now(); // Start time for measuring duration

        DocGenerator docGenerator = new DLDocGenerator();

        Flux.range(0, numTasks)
        .parallel()
        .runOn(Schedulers.newParallel("se-stresstool", numThreads)) 
        .flatMap(i -> Flux.range(0, numOperations)
                .flatMap(j -> { 
                    String documentId = prefix+i.toString()+ (start+j);
                    return collection.upsert(documentId, docGenerator.generateDoc(i));
                })
        ).doOnNext(result -> {
            accumuCounter.accumulateAndGet(1L, Long::sum);
            long cnt = counter.accumulateAndGet(1L, Long::sum);
            if (cnt % logAfter == 0) {
                Instant endTime = Instant.now();
                long durationMillis = Duration.between(startTime, endTime).toMillis();
                double opsPerSecond =  (cnt / durationMillis) * 1000.0;
                DecimalFormat f = new DecimalFormat("##.00");
                System.out.println("Write Operations per second: " + f.format(opsPerSecond));
                startTime = Instant.now(); // Reset start time for the next batch
                counter.set(0L);
            }
        })
        .sequential()
        .blockLast();
    }
}
