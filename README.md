[![license](http://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/udhos/sqspipe/blob/master/LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/udhos/sqspipe)](https://goreportcard.com/report/github.com/udhos/sqspipe)
[![Go Reference](https://pkg.go.dev/badge/github.com/udhos/sqspipe.svg)](https://pkg.go.dev/github.com/udhos/sqspipe)

# sqspipe

sqspipe continuously moves messages between aws sqs queues in a rate limited manner.

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

You can use `$ROLE_ARN_SRC` to specify a role to access the source queue, and `$ROLE_ARN_DST` to specify a role to access the destination queue.

The role in `$ROLE_ARN_SRC` must allow actions `sqs:ReceiveMessage` and `sqs:DeleteMessage` to source queue.

The role in `$ROLE_ARN_DST` must allow action `sqs:SendMessage` to destination queue.

## Docker

    ./docker/build.sh