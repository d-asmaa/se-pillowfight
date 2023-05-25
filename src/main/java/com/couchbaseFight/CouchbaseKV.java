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
    AtomicReference<Long> counter = new AtomicReference<Long>();
    AtomicReference<Long> accumuCounter = new AtomicReference<Long>();
    Instant startTime;
    public CouchbaseKV(ReactiveCollection collection) {
        this.collection = collection;
    }
    
    public void pushReadRequests(int numOperations, int numConcurrentRequests, int start, int end, String prefix){
        counter.set(0L); // Counter for number of operations
        accumuCounter.set(0L);
        startTime = Instant.now(); // Start time for measuring duration

        Flux.range(0, numConcurrentRequests)
        .parallel()
        .runOn(Schedulers.parallel())
        .flatMap(i -> Flux.range(0, numOperations)
                .flatMap(j -> {
                    //String documentId= prefix + ThreadLocalRandom.current().nextInt(start, end + 1);
                    if (accumuCounter.get() > numOperations) {
                        System.exit(0);
                    }
                    String documentId = "0000129d-e66f-46d9-a7d4-274a7930a967";
                    return Flux.just(documentId)
                            .flatMap(id -> (this.collection.get(id))
                                    .map(GetResult::contentAsObject)
                            );
                })
        ).doOnNext(result -> {
            accumuCounter.accumulateAndGet(1L, Long::sum);
            long cnt = counter.accumulateAndGet(1L, Long::sum);
            if (cnt % 10000L == 0) {
                Instant endTime = Instant.now();
                long durationMillis = Duration.between(startTime, endTime).toMillis();
                double opsPerSecond =  cnt / (durationMillis * 1000.0);
                DecimalFormat f = new DecimalFormat("##.00");
                System.out.println(Thread.currentThread().getName()+" \t Read Operations per second: " + f.format(opsPerSecond));
                startTime = Instant.now(); // Reset start time for the next batch
                counter.set(0L);
            }
        })
        .sequential()
        .blockLast();


    }

    public void pushWriteRequests(int numOperations, int numConcurrentRequests, String prefix){
        counter.set(0L); // Counter for number of operations
        accumuCounter.set(0L);
        startTime = Instant.now(); // Start time for measuring duration

        DocGenerator docGenerator = new DLDocGenerator();
        Flux.range(0, numConcurrentRequests)
        .parallel()
        .runOn(Schedulers.newParallel("asmaa-parallel", numConcurrentRequests)) // @Fabrice: 
        .flatMap(i -> Flux.range(0, numOperations)
                .flatMap(j -> { 
                    if ((long) accumuCounter.get() > numOperations) {
                        System.exit(0);
                    }
                   // String documentId = prefix +generateKey(i);
                   String documentId = UUID.randomUUID().toString();
                    return collection.upsert(documentId, docGenerator.generateDoc(i));
                })
        ).doOnNext(result -> {
            accumuCounter.accumulateAndGet(1L, Long::sum);
            long cnt = counter.accumulateAndGet(1L, Long::sum);
            if ( cnt % 10000L == 0) {
                Instant endTime = Instant.now();
                long durationMillis = Duration.between(startTime, endTime).toMillis();
                double opsPerSecond = (double) cnt / (double) durationMillis * 1000.0;
                DecimalFormat f = new DecimalFormat("##.00");
                System.out.println("Write Operations per second: " + f.format(opsPerSecond));
                startTime = Instant.now(); // Reset start time for the next batch
                counter.set(0L);
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
