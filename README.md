[![license](http://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/udhos/sqspipe/blob/main/LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/udhos/sqspipe)](https://goreportcard.com/report/github.com/udhos/sqspipe)
[![Go Reference](https://pkg.go.dev/badge/github.com/udhos/sqspipe.svg)](https://pkg.go.dev/github.com/udhos/sqspipe)

# sqspipe

sqspipe continuously moves messages between aws sqs queues in a rate limited manner.

## Features

* Use case is to put a rate limit in between SQS messaging.
* May be handy to move all messages from one queue to another in a single shot.
* One is supposed to run a single instance of sqsqueue. If you run multiple instances (like in multiple pods replicas), every instance will deliver the maximum configured rate.
* Moving messages across regions is supported.
* Moving messages across accounts is supported.
* Health check endpoint: http://localhost:2000/health
* Prometheus metrics endpoint: http://localhost:3000/metrics

## Build

    git clone https://github.com/udhos/sqspipe
    cd sqspipe
    go install ./sqspipe

## Run

    export QUEUE_URL_SRC=https://sqs.us-east-1.amazonaws.com/111111111111/queue_src
    export QUEUE_URL_DST=https://sqs.us-east-1.amazonaws.com/222222222222/queue_dst
    sqspipe

## Mandatory Env Vars

sqspipe will move messages from source queue defined in `$QUEUE_URL_SRC` to destination queue defined in `$QUEUE_URL_DST`.

These env vars are required.

    export QUEUE_URL_SRC=https://sqs.us-east-1.amazonaws.com/111111111111/queue_src
    export QUEUE_URL_DST=https://sqs.us-east-1.amazonaws.com/222222222222/queue_dst

## Optional Env Vars

These env vars are optional.

    export ROLE_ARN_SRC=arn:aws:iam::111111111111:role/sqs_consumer
    export ROLE_ARN_DST=arn:aws:iam::222222222222:role/sqs_producer
    export MAX_RATE=16 ;# max messages per second
    export HEALTH_ADDR=:2000
    export HEALTH_PATH=/health
    export METRICS_ADDR=:3000
    export METRICS_PATH=/metrics

### Roles

You can use `$ROLE_ARN_SRC` to specify a role to access the source queue, and `$ROLE_ARN_DST` to specify a role to access the destination queue. The role in `$ROLE_ARN_SRC` must allow actions `sqs:ReceiveMessage` and `sqs:DeleteMessage` to source queue. The role in `$ROLE_ARN_DST` must allow action `sqs:SendMessage` to destination queue.

### Max Rate

If `$MAX_RATE` isn't specified, it defaults to 16 messages per second.

## Health check

You can use these environment variables to define the health check endpoint.

    HEALTH_ADDR=:2000
    HEALTH_PATH=/health

## Metrics

You can use these environment variables to define the metrics endpoint.

    METRICS_ADDR=:3000
    METRICS_PATH=/metrics

The metrics endpoint exposes the variables below.

```
# HELP sqspipe_delete_error_total Number of SQS DeleteMessage errors.
# TYPE sqspipe_delete_error_total counter
sqspipe_delete_error_total 0

# HELP sqspipe_delete_ok_total Number of successful SQS DeleteMessage calls.
# TYPE sqspipe_delete_ok_total counter
sqspipe_delete_ok_total 0

# HELP sqspipe_read_empty_total Number of empty SQS listener ReceiveMessage calls.
# TYPE sqspipe_read_empty_total counter
sqspipe_read_empty_total 0

# HELP sqspipe_read_error_total Number of SQS listener ReceiveMessage errors.
# TYPE sqspipe_read_error_total counter
sqspipe_read_error_total 0

# HELP sqspipe_read_message_total Number of messages read successfully.
# TYPE sqspipe_read_message_total counter
sqspipe_read_message_total 0

# HELP sqspipe_read_ok_total Number of successful SQS listener ReceiveMessage calls.
# TYPE sqspipe_read_ok_total counter
sqspipe_read_ok_total 0

# HELP sqspipe_write_error_total Number of SQS SendMessage errors.
# TYPE sqspipe_write_error_total counter
sqspipe_write_error_total 0

# HELP sqspipe_write_ok_total Number of successful SQS SendMessage calls.
# TYPE sqspipe_write_ok_total counter
sqspipe_write_ok_total 0
```

## Docker

Build recipe:

    ./docker/build.sh

Pull from Docker hub:

    docker pull udhos/sqspipe:0.0.0

Docker hub: https://hub.docker.com/r/udhos/sqspipe
