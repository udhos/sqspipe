package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

const (
	version  = "0.0.0"
	basename = "sqspipe"
)

type clientConfig struct {
	sqs      *sqs.Client
	queueURL string
}

type appConfig struct {
	src             clientConfig
	dst             clientConfig
	waitTimeSeconds int32
	pipeSrc         chan types.Message
	pipeDst         chan types.Message
	readers         int
	writers         int
	maxRate         int // messages per second
	interval        time.Duration
}

func main() {

	log.Printf("%s version=%s runtime=%s GOOS=%s GOARCH=%s GOMAXPROCS=%d", basename, version, runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0))

	app := appConfig{
		waitTimeSeconds: 10, // 0..20
		pipeSrc:         make(chan types.Message, 10),
		pipeDst:         make(chan types.Message, 0),
		readers:         valueFromEnv("READERS", 1),
		writers:         valueFromEnv("WRITERS", 1),
		maxRate:         valueFromEnv("MAX_RATE", 16), // messages per second
		interval:        500 * time.Millisecond,
	}

	app.src = initClient(requireEnv("QUEUE_URL_SRC"), getEnv("ROLE_ARN_SRC"))
	app.dst = initClient(requireEnv("QUEUE_URL_DST"), getEnv("ROLE_ARN_DST"))

	run(app)
}

func valueFromEnv(name string, defaultValue int) int {
	if str := getEnv(name); str != "" {
		value, errConv := strconv.Atoi(str)
		if errConv == nil {
			return value
		}
		log.Fatalf("bad %s=[%s]: %v", name, str, errConv)
		os.Exit(1)
	}
	return defaultValue
}

func getEnv(name string) string {
	value := os.Getenv(name)
	log.Printf("%s=[%s]", name, value)
	return value
}

func requireEnv(name string) string {
	value := getEnv(name)
	if value == "" {
		log.Fatalf("requireEnv: please set env var: %s\n", name)
		os.Exit(1)
		return ""
	}

	return value
}

func initClient(queueURL, roleArn string) clientConfig {

	var c clientConfig

	region, errRegion := queueRegion(queueURL)
	if errRegion != nil {
		log.Fatalf("initClient: %v\n", errRegion)
		os.Exit(1)
		return c
	}

	cfg, errConfig := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if errConfig != nil {
		log.Fatalf("initClient: %v\n", errConfig)
		os.Exit(1)
		return c
	}

	if roleArn != "" {
		clientSts := sts.NewFromConfig(cfg)
		cfg2, errConfig2 := config.LoadDefaultConfig(
			context.TODO(), config.WithRegion(region),
			config.WithCredentialsProvider(aws.NewCredentialsCache(
				stscreds.NewAssumeRoleProvider(
					clientSts,
					roleArn,
				)),
			),
		)
		if errConfig2 != nil {
			log.Fatalf("initClient: AssumeRole %s: %v\n", roleArn, errConfig2)
			os.Exit(1)
			return c

		}
		cfg = cfg2
	}

	c = clientConfig{
		sqs:      sqs.NewFromConfig(cfg),
		queueURL: queueURL,
	}

	return c
}

// https://sqs.us-east-1.amazonaws.com/123456789012/myqueue
func queueRegion(queueURL string) (string, error) {
	fields := strings.SplitN(queueURL, ".", 3)
	if len(fields) < 3 {
		return "", fmt.Errorf("queueRegion: bad queue url=[%s]", queueURL)
	}
	region := fields[1]
	log.Printf("queueRegion=[%s]", region)
	return region, nil
}

func run(app appConfig) {
	wg := &sync.WaitGroup{}

	log.Printf("run: readers=%d writers=%d", app.readers, app.writers)

	wg.Add(app.readers + 1 + app.writers)

	for i := 0; i < app.readers; i++ {
		go reader(i, wg, app)
	}
	go limiter(wg, app)
	for i := 0; i < app.writers; i++ {
		go writer(i, wg, app)
	}

	wg.Wait()
}

func reader(id int, wg *sync.WaitGroup, app appConfig) {

	me := fmt.Sprintf("reader[%d]", id)

	src := app.src

	defer wg.Done()

	for {

		//
		// receive from source queue
		//

		input := &sqs.ReceiveMessageInput{
			QueueUrl: &src.queueURL,
			AttributeNames: []types.QueueAttributeName{
				"SentTimestamp",
			},
			MaxNumberOfMessages: 10, // 1..10
			MessageAttributeNames: []string{
				"All",
			},
			WaitTimeSeconds: app.waitTimeSeconds,
		}

		log.Printf("%s: waiting", me)

		resp, errRecv := src.sqs.ReceiveMessage(context.TODO(), input)
		if errRecv != nil {
			log.Printf("%s: ReceiveMessage: %v", me, errRecv)
			continue
		}

		count := len(resp.Messages)

		log.Printf("%s: found %d messages", me, count)

		pipeLen(me, app)

		//
		// send to limiter
		//

		for i, msg := range resp.Messages {
			log.Printf("%s: %d/%d MessageId: %s", me, i+1, count, *msg.MessageId)
			app.pipeSrc <- msg
		}
	}

}

func pipeLen(label string, app appConfig) {
	log.Printf("%s: pipeSrc=%d/%d pipeDst=%d/%d", label, len(app.pipeSrc), cap(app.pipeSrc), len(app.pipeDst), cap(app.pipeDst))
}

func limiter(wg *sync.WaitGroup, app appConfig) {

	me := "limiter"

	defer wg.Done()

	maxRate := app.maxRate // messages per second
	interval := app.interval
	intervalQuota := int(time.Duration(maxRate) * interval / time.Second)

	log.Printf("limiter: rate=%v/sec interval=%v quota=%v/interval", maxRate, interval, intervalQuota)

	begin := time.Now()
	sent := 0

	for {

		//
		// get message from reader
		//

		pipeLen(me, app)
		log.Printf("%s: waiting", me)
		m := <-app.pipeSrc
		elap := time.Since(begin)

		log.Printf("%s: src: MessageId: %s - elap=%v sent=%d", me, *m.MessageId, elap, sent)

		if elap >= interval {
			// elap >= interval: send and restart interval
			forward(app.pipeDst, m) // send to writer
			begin = time.Now()
			sent = 1
			continue
		}

		// elap < interval: within interval

		if sent >= intervalQuota {
			// quota exceeded: wait and restart interval
			hold := interval - elap
			log.Printf("%s: hold %v", me, hold)
			time.Sleep(hold)
			begin = time.Now()
			sent = 0
		}

		// send
		forward(app.pipeDst, m) // send to writer
		sent++
	}

}

func forward(pipe chan types.Message, m types.Message) {
	me := "limiter"
	log.Printf("%s: %s sending...", me, *m.MessageId)
	pipe <- m
	log.Printf("%s: %s sending...done ", me, *m.MessageId)
}

func writer(id int, wg *sync.WaitGroup, app appConfig) {

	me := fmt.Sprintf("writer[%d]", id)

	dst := app.dst

	defer wg.Done()

	for {

		//
		// read from limiter
		//

		pipeLen(me, app)
		log.Printf("%s: waiting", me)
		m := <-app.pipeDst
		log.Printf("%s: MessageId: %s", me, *m.MessageId)

		//
		// write into destination queue
		//

		input := &sqs.SendMessageInput{
			QueueUrl:          &app.dst.queueURL,
			DelaySeconds:      0, // 0..900
			MessageAttributes: m.MessageAttributes,
			MessageBody:       m.Body,
		}

		_, errSend := dst.sqs.SendMessage(context.TODO(), input)
		if errSend != nil {
			log.Printf("%s: MessageId: %s - SendMessage: %v", me, *m.MessageId, errSend)
			continue
		}

		//
		// delete from source queue
		//

		inputDelete := &sqs.DeleteMessageInput{
			QueueUrl:      &app.src.queueURL,
			ReceiptHandle: m.ReceiptHandle,
		}

		_, errDelete := app.src.sqs.DeleteMessage(context.TODO(), inputDelete)
		if errDelete != nil {
			log.Printf("%s: MessageId: %s - DeleteMessage: %v", me, *m.MessageId, errDelete)
			continue
		}
	}

}
