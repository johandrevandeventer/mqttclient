package mqttclient

import (
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// MessageQueue handles queuing and processing messages
type MessageQueue struct {
	queue    chan mqtt.Message
	workers  int
	wg       sync.WaitGroup
	stopChan chan struct{}
}

// NewMessageQueue creates a new message queue with a specified buffer size
func NewMessageQueue(bufferSize int, workers int) *MessageQueue {
	return &MessageQueue{
		queue:    make(chan mqtt.Message, bufferSize),
		workers:  workers,
		stopChan: make(chan struct{}),
	}
}

// Enqueue adds a message to the queue
func (mq *MessageQueue) Enqueue(msg mqtt.Message) {
	mq.queue <- msg
}

// StartProcessing starts worker goroutines to process messages from the queue
func (mq *MessageQueue) StartProcessing(handler func(mqtt.Message)) {
	for i := 0; i < mq.workers; i++ {
		mq.wg.Add(1)
		go mq.worker(handler)
	}
}

// worker processes messages from the queue
func (mq *MessageQueue) worker(handler func(mqtt.Message)) {
	defer mq.wg.Done()
	for {
		select {
		case msg := <-mq.queue:
			handler(msg)
		case <-mq.stopChan:
			return
		}
	}
}

// StopProcessing signals workers to stop and waits for them to finish
func (mq *MessageQueue) StopProcessing() {
	close(mq.stopChan)
	mq.wg.Wait()
	close(mq.queue) // Clean up the queue
}
