
[Todo: drone.io badge]

sg-sync is a tool that can drive a replication between two [Sync Gateway](https://github.com/couchbase/sync_gateway) instances.  

It was created because the Sync Gateway can only serve as a passive replication target, but it does not offer a mechanism to drive a replication.

![architecture](http://tleyden-misc.s3.amazonaws.com/blog_images/sg-sync-architecture.png)

# Features

* Json configuration file to specify replications
* Supports multiple replications running concurrently 
* Possible to specify OneShot and Continuous replications
* Solid unit test coverage

# Limitations

* Only works on recent versions of Sync Gateway (after commit 50d30eb3d on March 7, 2014)

* Cannot do filtered replications yet
	
# Documentation

* This README
* [TODO -- add link to godocs]

# Sample Configuration

```
{
    "changes_feed_limit": 50,
    "continuous_retry_time_ms": 15000,
    "replications":{
        "db-local":{
            "source_url": "http://localhost:4985",
            "source_db": "db",
            "target_db": "db-copy",
            "lifecycle": "oneshot"
        },
        "grocery-sync-local":{
            "source_url": "http://localhost:4985",
            "target_url": "http://sync.couchbasecloud.com",
            "source_db": "grocery-sync",
            "target_db": "grocery-sync",
            "lifecycle": "continuous"
        }
    }
}
```

# Configuration fields

* `changes_feed_limit` -- the number of changes to fetch in each "batch".  Setting this to a larger value will increase memory consumption.
* `continuous_retry_time_ms` -- the amount of time to wait after an aborted replication before retrying.  (only applicable to continuous replications)
* `replications` -- a "map" of replications, where each replication has a unique name.  they will be run in the order given in this file.
* `source_url` -- url of source sync gateway, **without** the db name in the url.
* `source_db` -- the source db to pull from.
* `target_url` -- url of target sync gateway, **without** the db name in the url.  If omitted, it will be assumed that it's the same as the `source_url`
* `target_db` -- the target db to push to.  
* `lifecycle` -- possible values: `oneshot` or `continuous`.  
     * `oneshot` replications will be run inline (synchronously), and it will not start the following replications until the oneshot replication completes.  
     * `continuous` replications run asynchronously and indefinitely.


# Prerequisites

* [Go](http://golang.org/doc/install) is required if you are building from source

# Running sg-sync

* cd into <sg-sync>/cli directory
* Take the config.json.example file and rename it to config.json
* Customize it to have your sync gateway URL's
* Run it:

```
$ ./cli 
```

*Notes on command behavior*:

* If you have any continuous replications, the command will block indefinitely.  
* If you only have oneshot replications defined, the command will exit once they have all completed.

# Todo Items

* Logs are difficult to disentangle when multiple replications are running -- workaround: use separate instances and separate config files for each replication
* Integration test with actual sync gateway (unit tests run against a mock sync gateway)
* Clean up API to only expose what's necessary
* Attachments handling should be made to be more efficient.  Currently, attachment data is temporarily stored in memory before it is pushed to the target server.
