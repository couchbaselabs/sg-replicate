
[Todo: drone.io badge]

sg-sync is a tool that can drive a replication between two Sync Gateways.  

It was created because the Sync Gateway can only serve as a passive replication target, but it does not offer a mechanism to drive a replication.

![architecture](http://tleyden-misc.s3.amazonaws.com/blog_images/sg-sync-architecture.png)

# Features

* One Shot replications [TODO: extend CLI to support this]
* Continuous replications
* Json configuration file to specify replications
* Supports multiple replications
* Unit test suite

# Sample Configuration

```
{
    "changes_feed_limit": 50,
    "continuous_retry_time_ms": 15000,
    "replications":{
        "db-local":{
            "source_url": "http://localhost:4985",
            "source_db": "db",
            "target_db": "db-copy"
        },
        "grocery-sync-local":{
            "source_url": "http://localhost:4985",
            "target_url": "http://sync.couchbasecloud.com",
            "source_db": "grocery-sync",
            "target_db": "grocery-sync"
        }
    }
}
```

# Limitations

* Only works on recent versions of Sync Gateway

# Pre-requisites

* [Go](http://golang.org/doc/install) is required if you are building from source
* When binary versions become available, there will be no pre-requisites.

# Run sg-sync

* cd into cli directory
* Take the config.json.example file and rename it to config.json
* Customize it to have your sync gateway URL's
* Run it

```
$./cli 
```

# Documentation

* This README
* [TODO -- add link to godocs]

# Todo Items

* Logs are impossible to disentangle when multiple replications are running -- workaround: use separate instances and separate config files for each replication
* Integration test with actual sync gateway 


