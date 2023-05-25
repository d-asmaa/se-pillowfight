package com.couchbaseFight;

import com.couchbase.client.java.json.JsonObject;

public interface DocGenerator {

    JsonObject generateDoc(int counter);
}
