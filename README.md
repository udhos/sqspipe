# sqspipe
continuously move messages between aws sqs queues in a rate limited manner

## Env vars

### Mandatory

    export QUEUE_URL_SRC=https://sqs.us-east-1.amazonaws.com/111111111111/queue_src
    export QUEUE_URL_DST=https://sqs.us-east-1.amazonaws.com/222222222222/queue_dst

### Optional

    export ROLE_ARN_SRC=arn:aws:iam::111111111111:role/sqs_consumer
    export ROLE_ARN_DST=arn:aws:iam::222222222222:role/sqs_producer
    export READERS=1
    export WRITERS=1
    export MAX_RATE=16
