# se-pillowfight

This is a project to test a Couchbase cluster with a parametrized workload

use it with

```
docker run marcobevilacqua94/se_pillowfight:latest java -jar se_pillowfight.jar -h (host) -u (username) -p (password) -b (bucket) -s (scope) -c (collection) -nr (num_read) -nw (num_write) -sk (start_key_rage) -ek (end_key_range) -pr (prefix_key) -tpt (tasks_per_thread) -th (num_threads) -sep (separator) -lg (log-after)
```

default values for parameters are
```
host: 127.0.0.1
username: cbuser
password: password
bucket: appointment
scope: _default
collection: source
num_read: 0
num_write: 1000000
start_key_range: 0
end_key_range: 499999
prefix_key: cb
task_per_thread = 2
num_threads: 12
separator: _
log-after: 1000 
```