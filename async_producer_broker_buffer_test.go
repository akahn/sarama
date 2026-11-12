package sarama

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blackholeListener accepts TCP connections and never reads from them,
// imitating a sick broker: produce requests pile up in the OS socket buffer
// until the client's conn write blocks, and no response ever arrives.
type blackholeListener struct {
	ln    net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func newBlackholeListener(t *testing.T) *blackholeListener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &blackholeListener{ln: ln}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			b.mu.Lock()
			b.conns = append(b.conns, conn)
			b.mu.Unlock()
		}
	}()
	return b
}

func (b *blackholeListener) close() {
	b.ln.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, c := range b.conns {
		c.Close()
	}
}

// sickBrokerProducer builds an AsyncProducer against two brokers: a healthy
// MockBroker leading t/0 and a blackholed listener leading t/1.
func sickBrokerProducer(t *testing.T, conf *Config) (AsyncProducer, *MockBroker, *blackholeListener) {
	t.Helper()
	healthy := NewMockBroker(t, 1)
	sick := newBlackholeListener(t)

	metadata := NewMockMetadataResponse(t).
		SetBroker(healthy.Addr(), healthy.BrokerID()).
		SetBroker(sick.ln.Addr().String(), 2).
		SetLeader("t", 0, healthy.BrokerID()).
		SetLeader("t", 1, 2)
	produce := NewMockProduceResponse(t).SetError("t", 0, ErrNoError)
	healthy.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": metadata,
		"ProduceRequest":  produce,
	})

	conf.Version = V2_1_0_0
	conf.ApiVersionsRequest = false // the blackhole cannot answer the handshake
	conf.Producer.Partitioner = NewManualPartitioner
	conf.Producer.Return.Successes = true
	conf.Producer.Return.Errors = true
	conf.Producer.Flush.Frequency = 10 * time.Millisecond
	conf.Producer.Flush.Messages = 4
	conf.Producer.MaxMessageBytes = 64 * 1024
	// keep the sick broker stalled for the whole measurement window
	conf.Net.ReadTimeout = 120 * time.Second
	conf.Net.WriteTimeout = 120 * time.Second
	conf.Metadata.RefreshFrequency = 0

	producer, err := NewAsyncProducer([]string{healthy.Addr()}, conf)
	if err != nil {
		t.Fatal(err)
	}
	return producer, healthy, sick
}

// Without MaxBrokerBufferBytes a single stalled broker back-pressures the
// shared dispatch stages until Input() stops accepting messages for every
// partition (head-of-line blocking).
func TestAsyncProducerStalledBrokerBlocksAllPartitions(t *testing.T) {
	conf := NewConfig()
	conf.ChannelBufferSize = 8
	producer, healthy, sick := sickBrokerProducer(t, conf)
	defer healthy.Close()

	var healthyDelivered atomic.Int64
	var drainWG sync.WaitGroup
	drainWG.Add(2)
	go func() {
		defer drainWG.Done()
		for msg := range producer.Successes() {
			if msg.Partition == 0 {
				healthyDelivered.Add(1)
			}
		}
	}()
	go func() {
		defer drainWG.Done()
		for range producer.Errors() {
		}
	}()

	value := ByteEncoder(make([]byte, 4096))
	accepted := 0
sendLoop:
	for i := 0; i < 10000; i++ {
		msg := &ProducerMessage{Topic: "t", Partition: int32(i % 2), Value: value}
		select {
		case producer.Input() <- msg:
			accepted++
		case <-time.After(2 * time.Second):
			break sendLoop // stalled, as expected with the limit disabled
		}
	}
	if accepted == 10000 {
		t.Fatalf("expected Input() to stall with MaxBrokerBufferBytes disabled; all %d sends accepted", accepted)
	}

	// healthy-partition deliveries freeze too: messages for the healthy broker
	// are stuck behind the stalled partition in the shared topic dispatcher
	before := healthyDelivered.Load()
	time.Sleep(2 * time.Second)
	if after := healthyDelivered.Load(); after != before {
		t.Errorf("healthy deliveries advanced from %d to %d during the stall", before, after)
	}
	t.Logf("stall after %d accepted messages; healthy delivered %d", accepted, healthyDelivered.Load())

	sick.close() // break the stall so shutdown can proceed
	producer.AsyncClose()
	drainWG.Wait()
}

// With MaxBrokerBufferBytes set, messages for the stalled broker are parked and
// eventually shed with ErrProducerBrokerBufferOverflow, while traffic to the
// healthy broker keeps flowing.
func TestAsyncProducerStalledBrokerShedsAndHealthyFlows(t *testing.T) {
	conf := NewConfig()
	conf.ChannelBufferSize = 8
	conf.Producer.MaxBrokerBufferBytes = 128 * 1024
	producer, healthy, sick := sickBrokerProducer(t, conf)
	defer healthy.Close()
	defer sick.close()

	var healthyDelivered, overflowErrs, otherErrs atomic.Int64
	var drainWG sync.WaitGroup
	drainWG.Add(2)
	go func() {
		defer drainWG.Done()
		for msg := range producer.Successes() {
			if msg.Partition == 0 {
				healthyDelivered.Add(1)
			}
		}
	}()
	go func() {
		defer drainWG.Done()
		for perr := range producer.Errors() {
			if errors.Is(perr.Err, ErrProducerBrokerBufferOverflow) {
				overflowErrs.Add(1)
			} else {
				otherErrs.Add(1)
			}
		}
	}()

	value := ByteEncoder(make([]byte, 4096))
	const total = 2000
	// paced below the healthy broker's capacity: the shed path is for stalled
	// brokers, not for load the broker could have absorbed
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for i := 0; i < total; i++ {
		if i%10 == 0 {
			<-ticker.C
		}
		msg := &ProducerMessage{Topic: "t", Partition: int32(i % 2), Value: value}
		select {
		case producer.Input() <- msg:
		case <-time.After(2 * time.Second):
			t.Fatalf("Input() blocked at message %d despite MaxBrokerBufferBytes", i)
		}
	}

	deadline := time.After(10 * time.Second)
	for healthyDelivered.Load() < total/2 {
		select {
		case <-deadline:
			t.Fatalf("healthy deliveries stalled at %d of %d (overflowErrs=%d otherErrs=%d)",
				healthyDelivered.Load(), total/2, overflowErrs.Load(), otherErrs.Load())
		case <-time.After(50 * time.Millisecond):
		}
	}
	if overflowErrs.Load() == 0 {
		t.Error("expected sick-partition messages to be shed with ErrProducerBrokerBufferOverflow")
	}
	t.Logf("healthy delivered %d/%d; shed %d; other errors %d",
		healthyDelivered.Load(), total/2, overflowErrs.Load(), otherErrs.Load())

	// Break the stalled conns first: shutdown's final flush would otherwise
	// block on the dead socket until Net.WriteTimeout.
	sick.close()
	producer.AsyncClose()
	drainWG.Wait()
}

// A retriable produce error that arrives while messages are parked exercises
// drainOverflow's retry branch: parked messages must re-enter behind the
// dropped-and-retried batch, preserving per-partition order end to end.
func TestAsyncProducerParkedMessagesRetryInOrder(t *testing.T) {
	broker := NewMockBroker(t, 1)
	defer broker.Close()
	// Slow responses keep the first batch in flight while the burst below
	// parks behind it.
	broker.SetLatency(100 * time.Millisecond)

	metadata := NewMockMetadataResponse(t).
		SetBroker(broker.Addr(), broker.BrokerID()).
		SetLeader("t", 0, broker.BrokerID())
	notLeader := NewMockProduceResponse(t).SetError("t", 0, ErrNotLeaderForPartition)
	produceOK := NewMockProduceResponse(t).SetError("t", 0, ErrNoError)
	broker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": metadata,
		"ProduceRequest":  NewMockSequence(notLeader, produceOK),
	})

	conf := NewConfig()
	conf.Version = V2_1_0_0
	conf.ApiVersionsRequest = false // MockBroker has no ApiVersions handler
	conf.ChannelBufferSize = 8
	conf.Producer.Partitioner = NewManualPartitioner
	conf.Producer.Return.Successes = true
	conf.Producer.Return.Errors = true
	conf.Producer.Flush.Frequency = 10 * time.Millisecond
	conf.Producer.MaxMessageBytes = 64 * 1024
	conf.Producer.MaxBrokerBufferBytes = 16 * 1024 * 1024
	producer, err := NewAsyncProducer([]string{broker.Addr()}, conf)
	if err != nil {
		t.Fatal(err)
	}

	const total = 300
	var (
		mu    sync.Mutex
		order []int
	)
	var errCount atomic.Int64
	var drainWG sync.WaitGroup
	drainWG.Add(2)
	go func() {
		defer drainWG.Done()
		for msg := range producer.Successes() {
			mu.Lock()
			order = append(order, msg.Metadata.(int))
			mu.Unlock()
		}
	}()
	go func() {
		defer drainWG.Done()
		for perr := range producer.Errors() {
			errCount.Add(1)
			t.Logf("unexpected error: %v", perr.Err)
		}
	}()

	value := ByteEncoder(make([]byte, 4096))
	for i := 0; i < total; i++ {
		producer.Input() <- &ProducerMessage{Topic: "t", Partition: 0, Value: value, Metadata: i}
	}

	deadline := time.After(20 * time.Second)
	for {
		mu.Lock()
		delivered := len(order)
		mu.Unlock()
		if delivered+int(errCount.Load()) >= total {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("stalled with %d of %d delivered", delivered, total)
		case <-time.After(50 * time.Millisecond):
		}
	}
	producer.AsyncClose()
	drainWG.Wait()

	if n := errCount.Load(); n != 0 {
		t.Fatalf("expected no errors, got %d", n)
	}
	if len(order) != total {
		t.Fatalf("expected %d successes, got %d", total, len(order))
	}
	for i, got := range order {
		if got != i {
			t.Fatalf("out-of-order delivery at position %d: got message %d", i, got)
		}
	}
}
