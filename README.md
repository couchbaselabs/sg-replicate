	
[![Join the chat at https://gitter.im/couchbase/mobile](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/couchbase/discuss?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![Build Status](http://drone.couchbase.io/api/badges/couchbaselabs/sg-replicate/status.svg)](http://drone.couchbase.io/couchbaselabs/sg-replicate) [![GoDoc](https://godoc.org/github.com/couchbaselabs/sg-replicate?status.png)](https://godoc.org/github.com/couchbaselabs/sg-replicate) [![Coverage Status](https://coveralls.io/repos/couchbaselabs/sg-replicate/badge.svg?branch=master)](https://coveralls.io/r/couchbaselabs/sg-replicate?branch=master) [![Go Report Card](https://goreportcard.com/badge/github.com/couchbaselabs/sg-replicate)](https://goreportcard.com/report/github.com/couchbaselabs/sg-replicate) [![codebeat badge](https://codebeat.co/badges/7bd2f9ec-1313-421f-aa32-aa7fb7026432)](https://codebeat.co/projects/github-com-couchbaselabs-sg-replicate)

sg-replicate is a tool that can drive a replication between [Sync Gateway](https://github.com/couchbase/sync_gateway) instances.  

It was created because the Sync Gateway can only serve as a passive replication target, but it does not offer a mechanism to drive a replication.  This tool attempts to fill that gap.

![architecture](http://tleyden-misc.s3.amazonaws.com/blog_images/sg-replicate-architecture.png)

# Sync Gateway integration

sg-replicate has been integrated into Sync Gateway itself, and that is probably how you want to use it, unless you have a specific need to run it standalone.  See the documentation on [Sync Gateway Replications](https://developer.couchbase.com/documentation/mobile/1.3/guides/sync-gateway/running-replications/index.html) for usage instructions.

# Quickstart - build from source

*Note: there is no need to clone this repo, because the `go get` command below will take care of that for you*

* [Install Go](http://golang.org/doc/install) and define `$GOPATH` environment variable (eg, `export GOPATH=~/gocode`)
* `$ go get -u -v github.com/couchbaselabs/sg-replicate/...` -- clones this repo and all source dependencies into `$GOPATH/src`
* `$ cd ${GOPATH}/src/github.com/couchbaselabs/sg-replicate/cli`
* `$ cp config.json.example config.json`
* Customize `config.json` to contain the server URLs of your source and target server.
* Build and run:

```
$ go build && ./cli
```

*Notes on command behavior*:

* If you have any continuous replications, the command will block indefinitely, and only exit if there is a non-recoverable error with a continuous replication.
* If you only have oneshot replications defined, the command will exit once they have all completed.

# Features

* Json configuration file to specify replications
* Supports multiple replications running concurrently 
* Can run both OneShot and Continuous replications
* Does not store anything persistently
* Stateless -- can be interrupted/restarted anytime without negative side effects.
* Filter replications using channels.

# Documentation

[![GoDoc](https://godoc.org/github.com/couchbaselabs/sg-replicate?status.png)](https://godoc.org/github.com/couchbaselabs/sg-replicate)

* This README
* [GoDoc](http://godoc.org/github.com/couchbaselabs/sg-replicate)
* [Replication Algorithm](https://github.com/couchbaselabs/TouchDB-iOS/wiki/Replication-Algorithm) + [ladder diagram](http://cl.ly/image/1v013o210345)

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
            "lifecycle": "continuous",
            "channels": ["lists", "items"]
        }
    }
}
```

# Configuration fields

* `changes_feed_limit` -- the number of changes to fetch in each "batch".  Setting this to a larger value will increase memory consumption.
* `continuous_retry_time_ms` -- the amount of time to wait (in milliseconds) after an aborted replication before retrying.  (only applicable to continuous replications)
* `replications` -- a "map" of replications, where each replication has a unique name.  they will be run in the order given in this file.
* `source_url` -- url of source sync gateway, **without** the db name in the url.  Can point to admin port (:4985) or user port (:4984), but be aware if you point it to the user port, you will probably need to set a username/pass in the url and will only replicate docs visible to that user.
* `source_db` -- the source db to pull from.
* `target_url` -- url of target sync gateway, **without** the db name in the url.  If omitted, it will be assumed that it's the same as the `source_url`  See `source_url` for discussion of which port to use.
* `target_db` -- the target db to push to.  
* `channels` -- the set of channels that replication should be restricted to.
* `disabled` -- is this replication currently disabled?  (true | false)
* `lifecycle` -- possible values: `oneshot` or `continuous`.  
     * `oneshot` replications will be run inline (synchronously), and it will not start the following replications until the oneshot replication completes.  
     * `continuous` replications are started in parallel with other continuous replications, and run indefinitely until they have a non-recoverable error.

# Filing Issues

Please file issues in the [Sync Gateway](https://github.com/couchbase/sync_gateway/issues) issue tracker.  sg-replicate is now an officially supported component of Sync Gateway.

# Todo

* Logs are difficult to disentangle when multiple replications are running -- workaround: use separate instances and separate config files for each replication
* Clean up API to only expose what's necessary
* Attachments handling should be made to be more efficient.  Currently, attachment data is temporarily stored in memory before it is pushed to the target server.
* targetCheckpointAddress needs to take more things into account when generating checkpoint address.
