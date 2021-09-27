package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

const (
	version  = "0.1.0"
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

func getVersion() string {
	return fmt.Sprintf("%s version=%s runtime=%s GOOS=%s GOARCH=%s GOMAXPROCS=%d", basename, version, runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0))
}

func main() {

	var showVersion bool

	flag.BoolVar(&showVersion, "version", showVersion, "show version")

	flag.Parse()

	if showVersion {
		fmt.Print(getVersion())
		fmt.Println()
		return
	}

	log.Print(getVersion())

	app := appConfig{
		waitTimeSeconds: 20, // 0..20
		pipeSrc:         make(chan types.Message, valueFromEnv("CHANNEL_BUF_SRC", 10)),
		pipeDst:         make(chan types.Message, valueFromEnv("CHANNEL_BUF_DST", 0)),
		readers:         valueFromEnv("READERS", 1),
		writers:         valueFromEnv("WRITERS", 1),
		maxRate:         valueFromEnv("MAX_RATE", 16), // messages per second
		interval:        500 * time.Millisecond,
	}

	app.src = initClient("src", requireEnv("QUEUE_URL_SRC"), getEnv("ROLE_ARN_SRC"))
	app.dst = initClient("dst", requireEnv("QUEUE_URL_DST"), getEnv("ROLE_ARN_DST"))

	run(app)
}

func valueFromEnv(name string, defaultValue int) int {
	str := os.Getenv(name)
	if str != "" {
		value, errConv := strconv.Atoi(str)
		if errConv == nil {
			log.Printf("%s=[%s] using %s=%d default=%d", name, str, name, value, defaultValue)
			return value
		}
		log.Fatalf("bad %s=[%s]: error: %v", name, str, errConv)
		os.Exit(1)
	}
	log.Printf("%s=[%s] using %s=%d default=%d", name, str, name, defaultValue, defaultValue)
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
		log.Fatalf("requireEnv: error: please set env var: %s", name)
		os.Exit(1)
		return ""
	}

	return value
}

func initClient(caller, queueURL, roleArn string) clientConfig {

	var c clientConfig

	region, errRegion := queueRegion(queueURL)
	if errRegion != nil {
		log.Fatalf("%s initClient: error: %v", caller, errRegion)
		os.Exit(1)
		return c
	}

	cfg, errConfig := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if errConfig != nil {
		log.Fatalf("%s initClient: error: %v", caller, errConfig)
		os.Exit(1)
		return c
	}

	if roleArn != "" {
		//
		// AssumeRole
		//
		log.Printf("%s initClient: AssumeRole: arn: %s", caller, roleArn)
		clientSts := sts.NewFromConfig(cfg)
		cfg2, errConfig2 := config.LoadDefaultConfig(
			context.TODO(), config.WithRegion(region),
			config.WithCredentialsProvider(aws.NewCredentialsCache(
				stscreds.NewAssumeRoleProvider(
					clientSts,
					roleArn,
					func(o *stscreds.AssumeRoleOptions) {
						o.RoleSessionName = basename
					},
				)),
			),
		)
		if errConfig2 != nil {
			log.Fatalf("%s initClient: AssumeRole %s: error: %v", caller, roleArn, errConfig2)
			os.Exit(1)
			return c

		}
		cfg = cfg2
	}

	{
		// show caller identity
		clientSts := sts.NewFromConfig(cfg)
		input := sts.GetCallerIdentityInput{}
		respSts, errSts := clientSts.GetCallerIdentity(context.TODO(), &input)
		if errSts != nil {
			log.Printf("%s initClient: GetCallerIdentity: error: %v", caller, errSts)
		}
		log.Printf("%s initClient: GetCallerIdentity: Account=%s ARN=%s UserId=%s", caller, *respSts.Account, *respSts.Arn, *respSts.UserId)
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

	log.Printf("run: readers=%d writers=%d", app.readers, app.writers)

	for i := 0; i < app.readers; i++ {
		go reader(i, app)
	}
	go limiter(app)
	for i := 0; i < app.writers; i++ {
		go writer(i, app)
	}

	<-make(chan struct{})
}

func reader(id int, app appConfig) {

	me := fmt.Sprintf("reader[%d]", id)

	src := app.src

	log.Printf("%s: ready", me)

	var readMessage, readOk, readEmpty, readError int

	for {

		//
		// receive from source queue
		//

		log.Printf("%s: readMessage=%d readOk=%d readEmpty=%d readError=%d channelSrc=%d channelDst=%d", me, readMessage, readOk, readEmpty, readError, len(app.pipeSrc), len(app.pipeDst))

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

		//log.Printf("%s: waiting", me)

		resp, errRecv := src.sqs.ReceiveMessage(context.TODO(), input)
		if errRecv != nil {
			readError++
			log.Printf("%s: ReceiveMessage: error: %v", me, errRecv)
			continue
		}

		readOk++

		count := len(resp.Messages)

		log.Printf("%s: readMessage=%d + found %d messages = readMessage=%d", me, readMessage, count, readMessage+count)

		if count == 0 {
			readEmpty++
			continue
		}

		readMessage += count

		//
		// send to limiter
		//

		for i, msg := range resp.Messages {
			log.Printf("%s: %d/%d MessageId: %s", me, i+1, count, *msg.MessageId)
			app.pipeSrc <- msg
		}
	}

}

func limiter(app appConfig) {

	me := "limiter"

	maxRate := app.maxRate // messages per second
	interval := app.interval
	intervalQuota := int(time.Duration(maxRate) * interval / time.Second)

	log.Printf("limiter: ready - rate=%v/sec interval=%v quota=%v/interval", maxRate, interval, intervalQuota)

	var forwarded int

	begin := time.Now()
	sent := 0

	for {

		//
		// get message from reader
		//

		log.Printf("%s: forwarded=%d channelSrc=%d channelDst=%d", me, forwarded, len(app.pipeSrc), len(app.pipeDst))

		m := <-app.pipeSrc
		elap := time.Since(begin)

		log.Printf("%s: src: MessageId: %s - elap=%v sent=%d", me, *m.MessageId, elap, sent)

		if elap >= interval {
			// elap >= interval: send and restart interval
			forward(app.pipeDst, m) // send to writer
			forwarded++
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
		forwarded++
		sent++
	}

}

func forward(pipe chan types.Message, m types.Message) {
	me := "limiter"
	pipe <- m
	log.Printf("%s: dst: MessageId: %s - sent", me, *m.MessageId)
}

func writer(id int, app appConfig) {

	me := fmt.Sprintf("writer[%d]", id)

	dst := app.dst

	log.Printf("%s: ready", me)

	var writeOk, writeError, deleteOk, deleteError int

	for {

		//
		// read from limiter
		//

		log.Printf("%s: writeOk=%d writeError=%d channelSrc=%d channelDst=%d", me, writeOk, writeError, len(app.pipeSrc), len(app.pipeDst))

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
			writeError++
			log.Printf("%s: MessageId: %s - SendMessage: error: %v", me, *m.MessageId, errSend)
			continue
		}

		writeOk++

		//
		// delete from source queue
		//

		inputDelete := &sqs.DeleteMessageInput{
			QueueUrl:      &app.src.queueURL,
			ReceiptHandle: m.ReceiptHandle,
		}

		_, errDelete := app.src.sqs.DeleteMessage(context.TODO(), inputDelete)
		if errDelete != nil {
			deleteError++
			log.Printf("%s: MessageId: %s - DeleteMessage: error: %v", me, *m.MessageId, errDelete)
			continue
		}

		deleteOk++
	}

}
