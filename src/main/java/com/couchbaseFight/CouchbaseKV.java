package com.couchbaseFight;

import java.util.concurrent.ThreadLocalRandom;

import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.GetResult;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class CouchbaseKV {
    ReactiveCollection collection;
    public CouchbaseKV(ReactiveCollection collection) {
        this.collection = collection;
    }
    
    public void pushReadRequests(int numIterations, int numConcurrentRequests, int start, int end, String prefix){
        Flux.range(0, numConcurrentRequests)
        .parallel()
        .runOn(Schedulers.parallel())
        .flatMap(i -> Flux.range(0, numIterations)
                .flatMap(j -> {
                    String documentId= prefix + ThreadLocalRandom.current().nextInt(start, end + 1);
                    return Flux.just(documentId)
                            .flatMap(id -> (this.collection.get(id))
                                    .map(GetResult::contentAsObject)
                            );
                })
        )
        .sequential()
        .blockLast();


    }

    public void pushWriteRequests(int numIterations, int numConcurrentRequests, String prefix){

        DocGenerator docGenerator = new DLDocGenerator();
        Flux.range(0, numConcurrentRequests)
        .parallel()
        .runOn(Schedulers.parallel())
        .flatMap(i -> Flux.range(0, numIterations)
                .flatMap(j -> { 
                    String documentId = prefix + i + "-" + j;
                    return collection.upsert(documentId, docGenerator.generateDoc(i));
                })
        )
        .sequential()
        .blockLast();
    }
}
