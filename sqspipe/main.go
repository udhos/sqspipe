// This is the main package for the sqspipe utility.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
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
	version  = "0.5.2"
	basename = "sqspipe"
)

type clientConfig struct {
	sqs      *sqs.Client
	queueURL string
}

type appConfig struct {
	src                clientConfig
	dst                clientConfig
	waitTimeSeconds    int32
	pipeSrc            chan types.Message
	pipeDst            chan types.Message
	readers            int
	writers            int
	maxRate            int // messages per second
	interval           int // milliseconds
	healthAddr         string
	healthPath         string
	readerHealth       []*readerHealthStat
	writerHealth       []*writerHealthStat
	errorCooldownRead  time.Duration
	errorCooldownWrite time.Duration
	metrics            *metrics
	metricsAddr        string
	metricsPath        string
	dynamicRateStep    int // messages per second
	dynamicRateTop     int // messages per second
	dynamicRatePeriod  int // seconds
}

func getVersion() string {
	return fmt.Sprintf("%s version=%s runtime=%s GOOS=%s GOARCH=%s GOMAXPROCS=%d", basename, version, runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0))
}

func main() {

	var showVersion bool
	var metricsNamespace string
	var metricsLabelKey string
	var metricsLabelValue string

	flag.BoolVar(&showVersion, "version", showVersion, "show version")
	flag.StringVar(&metricsNamespace, "metricsNamespace", basename, "set metrics namespace")
	flag.StringVar(&metricsLabelKey, "metricsLabelKey", metricsLabelKey, "set metrics label key")
	flag.StringVar(&metricsLabelValue, "metricsLabelValue", metricsLabelValue, "set metrics label value")

	flag.Parse()

	if showVersion {
		fmt.Print(getVersion())
		fmt.Println()
		return
	}

	log.Print(getVersion())

	log.Printf("metricsNamespace=[%s] metricsLabelKey=[%s] metricsLabelValue=[%s]", metricsNamespace, metricsLabelKey, metricsLabelValue)

	app := appConfig{
		waitTimeSeconds:    20, // 0..20
		pipeSrc:            make(chan types.Message, valueFromEnv("CHANNEL_BUF_SRC", 10)),
		pipeDst:            make(chan types.Message, valueFromEnv("CHANNEL_BUF_DST", 0)),
		readers:            valueFromEnv("READERS", 1),
		writers:            valueFromEnv("WRITERS", 1),
		maxRate:            valueFromEnv("MAX_RATE", 8),   // messages per second
		interval:           valueFromEnv("INTERVAL", 500), // millisecond
		src:                initClient("src", requireEnv("QUEUE_URL_SRC"), getEnv("ROLE_ARN_SRC")),
		dst:                initClient("dst", requireEnv("QUEUE_URL_DST"), getEnv("ROLE_ARN_DST")),
		healthAddr:         stringFromEnv("HEALTH_ADDR", ":2000"),
		healthPath:         stringFromEnv("HEALTH_PATH", "/health"),
		errorCooldownRead:  10 * time.Second,
		errorCooldownWrite: 10 * time.Second,
		metrics:            newMetrics(metricsNamespace, metricsLabelKey, metricsLabelValue),
		metricsAddr:        stringFromEnv("METRICS_ADDR", ":3000"),
		metricsPath:        stringFromEnv("METRICS_PATH", "/metrics"),
		dynamicRateStep:    valueFromEnv("DYNAMIC_RATE_STEP", 0),    // messages per second (0=disabled 2+=enabled)
		dynamicRateTop:     valueFromEnv("DYNAMIC_RATE_TOP", 16),    // messages per second
		dynamicRatePeriod:  valueFromEnv("DYNAMIC_RATE_PERIOD", 60), // seconds
	}

	lowestMaxRate := 1000 / app.interval

	log.Printf("lowestMaxRate=%d for INTERVAL=%d", lowestMaxRate, app.interval)

	if app.maxRate < lowestMaxRate {
		log.Printf("error: MAX_RATE=%d but lowestMaxRate=%d for INTERVAL=%d", app.maxRate, lowestMaxRate, app.interval)
		// will catch quota error below
	}

	intervalQuota := app.maxRate * app.interval / 1000
	if intervalQuota < 1 {
		log.Fatalf("error: intervalQuota=%d must be >= 1", intervalQuota)
		os.Exit(1)
	}

	run(app)
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
		} else {
			log.Printf("%s initClient: GetCallerIdentity: Account=%s ARN=%s UserId=%s", caller, *respSts.Account, *respSts.Arn, *respSts.UserId)
		}
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
		app.readerHealth = append(app.readerHealth, &readerHealthStat{id: i})
		go reader(i, app)
	}
	go limiter(app)
	for i := 0; i < app.writers; i++ {
		app.writerHealth = append(app.writerHealth, &writerHealthStat{id: i})
		go writer(i, app)
	}

	go serveMetrics(app.metricsAddr, app.metricsPath)

	startHealthEndpoint(app)

	<-make(chan struct{}) // wait forever
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
			app.metrics.readError.Inc()
			log.Printf("%s: ReceiveMessage: error: %v", me, errRecv)
			time.Sleep(app.errorCooldownRead)
			continue
		}

		readOk++
		app.metrics.readOk.Inc()
		app.readerHealth[id].update()

		count := len(resp.Messages)

		log.Printf("%s: found %d messages", me, count)

		if count == 0 {
			readEmpty++
			app.metrics.readEmpty.Inc()
			continue
		}

		readMessage += count
		app.metrics.readMessage.Add(float64(count))

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
	interval := time.Duration(app.interval) * time.Millisecond
	intervalQuotaBase := maxRate * app.interval / 1000
	currentQuota := intervalQuotaBase

	quotaStep := app.dynamicRateStep * app.interval / 1000
	quotaMax := app.dynamicRateTop * app.interval / 1000
	quotaPeriod := time.Duration(app.dynamicRatePeriod) * time.Second

	log.Printf("limiter: ready - rate=%v/sec interval=%v quota=%v/interval", maxRate, interval, intervalQuotaBase)

	var forwarded int

	var enforcementBegin time.Time
	var enforcementLast time.Time

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

		//
		// compute dynamic quota
		//
		{
			var enforcementElap time.Duration
			if time.Since(enforcementLast) > quotaPeriod {
				// enforcement was paused for longer than 1 minute
				enforcementBegin = time.Time{} // mark as no longer enforcing
				currentQuota = intervalQuotaBase
			} else {
				// enforcing
				enforcementElap = enforcementLast.Sub(enforcementBegin)
				currentQuota = intervalQuotaBase + quotaStep*int(enforcementElap/quotaPeriod)
				if currentQuota > quotaMax {
					currentQuota = quotaMax
				}
			}
			log.Printf("limiter: rate=%v/sec interval=%v enforce=%v quotaBase=%v quotaDynamic=%v", maxRate, interval, enforcementElap, intervalQuotaBase, currentQuota)
		}

		if elap >= interval {
			// elap >= interval: send and restart interval
			forward(app.pipeDst, m) // send to writer
			forwarded++
			begin = time.Now()
			sent = 1
			continue
		}

		// elap < interval: within interval

		if sent >= currentQuota {
			// quota exceeded: wait and restart interval
			hold := interval - elap
			log.Printf("%s: hold %v", me, hold)
			time.Sleep(hold)
			begin = time.Now()
			sent = 0

			//
			// start enforcing now
			//
			now := begin
			if enforcementBegin.IsZero() {
				enforcementBegin = now // start enforcing now
			}
			enforcementLast = now
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

	// initialize as healthy since it might take a long time before we get an actual message to deliver
	app.writerHealth[id].update(true)

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

		for {
			_, errSend := dst.sqs.SendMessage(context.TODO(), input)
			if errSend == nil {
				break
			}
			writeError++
			app.metrics.writeError.Inc()
			log.Printf("%s: MessageId: %s - SendMessage: error: %v", me, *m.MessageId, errSend)
			app.writerHealth[id].update(false)
			time.Sleep(app.errorCooldownWrite)
		}

		writeOk++
		app.metrics.writeOk.Inc()

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
			app.metrics.deleteError.Inc()
			log.Printf("%s: MessageId: %s - DeleteMessage: error: %v", me, *m.MessageId, errDelete)
			app.writerHealth[id].update(false)
			continue
		}

		deleteOk++
		app.metrics.deleteOk.Inc()
		app.writerHealth[id].update(true)
	}

}
