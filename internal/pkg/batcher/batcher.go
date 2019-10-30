package batcher

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/streadway/amqp"

	"github.com/kc0isg/azure-logging-shipper/internal/pkg/logshipper"
)

var (
	TotalBatchCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "azure_log_batch_operation_total",
		Help: "Total number of log batches sent to azure",
	})
	SuccessBatchCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "azure_log_batch_operation_success_total",
		Help: "Total number of successful log batches sent to azure",
	})
	FailedBatchCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "azure_log_batch_operation_failed_total",
		Help: "Total number of failed log batches sent to azure",
	})

	TotalCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "azure_log_operation_total",
		Help: "Total number of log events sent to azure",
	})
	SuccessCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "azure_log_operation_success_total",
		Help: "Total number of successful log events sent to azure",
	})
	FailedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "azure_log_operation_failed_total",
		Help: "Total number of failed log events sent to azure",
	})

	FlushCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "azure_log_batcher_flush_total",
		Help: "Total number of times the batcher has tried to flush",
	})
)

type Batcher struct {
	batch         *[]amqp.Delivery
	batchSize     int
	mux           sync.Mutex
	logShipper    *logshipper.AzureLogShipper
	flushInterval *time.Duration
	lastFlush     *time.Time
}

func NewBatcher(size int, shipper *logshipper.AzureLogShipper, flushInterval *time.Duration) *Batcher {
	batcher := &Batcher{
		batch:         &[]amqp.Delivery{},
		batchSize:     size,
		logShipper:    shipper,
		flushInterval: flushInterval,
	}

	if flushInterval.Seconds() > 0 {
		go batcher.startIntervalFlusher()
	}

	return batcher
}

func (b *Batcher) startIntervalFlusher() {
	for {
		select {
		case <-time.After(*b.flushInterval):
			b.mux.Lock()
			flushCheck := time.Now().Sub(*b.lastFlush) > *b.flushInterval
			b.mux.Unlock()

			if flushCheck {
				b.FlushBatch()
			}
		}
	}
}

func (b *Batcher) AddToBatch(delivery amqp.Delivery) {
	if len(*b.batch) >= b.batchSize {
		fmt.Printf("Flushing batch with size of %v\n", len(*b.batch))
		b.FlushBatch()
	}

	b.mux.Lock()

	*b.batch = append(*b.batch, delivery)

	b.mux.Unlock()
}

func (b *Batcher) FlushBatch() {
	FlushCounter.Inc()

	b.mux.Lock()

	batch := b.batch
	b.batch = &[]amqp.Delivery{}

	t := time.Now()
	b.lastFlush = &t

	b.mux.Unlock()

	go flush(batch, b.logShipper)

}

func flush(batch *[]amqp.Delivery, logShipper *logshipper.AzureLogShipper) {
	buffer := bytes.NewBuffer(make([]byte, 10000))
	buffer.Reset()
	buffer.WriteString("[")

	maxLen := len(*batch)

	for i, d := range *batch {
		buffer.Write(d.Body)

		if i < maxLen-1 {
			buffer.WriteString(",")
		}
	}

	buffer.WriteString("]")

	data := []byte(buffer.String())
	length := len(data)
	reader := bytes.NewReader(data)
	err := logShipper.SendLog("DemoExample2", length, reader)

	TotalBatchCounter.Inc()
	TotalCounter.Add(float64(maxLen))

	if err != nil {
		FailedCounter.Add(float64(maxLen))
		FailedBatchCounter.Inc()
	} else {
		SuccessCounter.Add(float64(maxLen))
		SuccessBatchCounter.Inc()
	}

	for _, d := range *batch {
		if err != nil {
			d.Reject(true)
		} else {
			d.Ack(false)
		}
	}
}
