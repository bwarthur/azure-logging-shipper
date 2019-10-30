package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/kc0isg/azure-logging-shipper/internal/pkg/batcher"
	"github.com/kc0isg/azure-logging-shipper/internal/pkg/logshipper"
	"github.com/kc0isg/azure-logging-shipper/internal/pkg/rabbit"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	workspaceID = flag.String("workspaceId", "", "Id for the Azure Log Analytics workspace")
	sharedKey   = flag.String("sharedKey", "", "Key for the Azure Log Analytics workspace")

	batchSize          = flag.Int("batch-size", 500, "Size of batches sent to Azure Logging Data Collector API")
	batchFlushInterval = flag.Duration("batch-flush-interval", 60*time.Second, "How long to wait before automatically flushing the batch")

	uri          = flag.String("uri", "amqp://dev:dev@corpelkd:5672/%2Flogging", "AMQP URI")
	exchangeName = flag.String("exchange", "qa.app.logs.default", "Durable, non-auto-deleted AMQP exchange name")
	exchangeType = flag.String("exchange-type", "fanout", "Exchange type - direct|fanout|topic|x-custom")
	createQueue  = flag.Bool("create-queue", false, "Create a new queue using the provided binding-key")
	queueName    = flag.String("queue", "test-queue", "Ephemeral AMQP queue name")
	bindingKey   = flag.String("binding-key", "#", "AMQP binding key")
	consumerTag  = flag.String("consumer-tag", "simple-consumer", "AMQP consumer tag (should not be blank)")
	lifetime     = flag.Duration("lifetime", 120*time.Second, "lifetime of process before shutdown (0s=infinite)")

	startTime time.Time
	endTime   time.Time
)

func init() {
	flag.Parse()
}

func main() {

	prometheus.Register(rabbit.TotalCounter)

	prometheus.Register(batcher.TotalCounter)
	prometheus.Register(batcher.SuccessCounter)
	prometheus.Register(batcher.FailedCounter)

	prometheus.Register(batcher.TotalBatchCounter)
	prometheus.Register(batcher.SuccessBatchCounter)
	prometheus.Register(batcher.FailedBatchCounter)

	startTime = time.Now()
	azureShipper := logshipper.AzureLogShipper{
		WorkspaceID: *workspaceID,
		SharedKey:   *sharedKey,
	}

	batcher := batcher.NewBatcher(*batchSize, &azureShipper, batchFlushInterval)

	c, err := rabbit.NewConsumer(*uri, *consumerTag, batcher)
	if err != nil {
		log.Fatalf("%s", err)
	}

	if *createQueue {
		queue, err := c.CreateQueue(*queueName)
		if err != nil {
			log.Fatalf("%s", err)
		}

		err = c.BindQueue(queue, *exchangeName, *bindingKey)
		if err != nil {
			log.Fatalf("%s", err)
		}
	}

	err = c.ConsumeQueue(*queueName)
	if err != nil {
		log.Fatalf("%s", err)
	}

	if *lifetime > 0 {
		log.Printf("running for %s", *lifetime)
		time.Sleep(*lifetime)
	} else {
		log.Printf("running forever")
		select {}
	}

	log.Printf("shutting down")

	if err := c.Shutdown(); err != nil {
		log.Printf("error during shutdown: %s\n", err)
	}

	endTime = time.Now()
	processResults()
}

func processResults() {
	gather, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		fmt.Printf("Error gathering metrics: %v", err)
	}

	opTotal, _ := GetCounter("azure_log_operation_total", gather)
	opSuccess, _ := GetCounter("azure_log_operation_success_total", gather)
	opFailed, _ := GetCounter("azure_log_operation_failed_total", gather)
	//opMttr, _ := GetHistogramSum("redis_operation_mttr_histogram_seconds", gather)

	opAvailability := opSuccess / opTotal
	runTimeSeconds := endTime.Sub(startTime).Seconds()
	rate := opTotal / runTimeSeconds

	fmt.Printf("azure_log_operation_total          => %f\n", opTotal)
	fmt.Printf("azure_log_operation_success_total  => %f\n", opSuccess)
	fmt.Printf("azure_log_operation_failed_total   => %f\n", opFailed)
	fmt.Printf("Availability                       => %f\n", opAvailability)
	fmt.Printf("Rate                               => %f\n", rate)
	//fmt.Printf("MTTR (seconds)                 => %f\n", opMttr)

	prometheus.WriteToTextfile("stats.txt", prometheus.DefaultGatherer)

	// for _, g := range gather {
	// 	MetricFamilyToText(os.Stdout, g)
	// }

	// for _, g := range gather {
	// 	fmt.Printf("Family Name: %s\n", g.GetName())
	// }

}
