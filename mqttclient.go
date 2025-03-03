package mqttclient

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

var messageChannel = make(chan mqtt.Message, 100)

// MQTTConfig is the configuration for the MQTT client
type MQTTConfig struct {
	Broker                string
	Port                  int
	ClientID              string
	Topic                 string
	Qos                   byte
	CleanSession          bool
	KeepAlive             int
	ReconnectOnDisconnect bool
	Username              string
	Password              string
}

// MQTTClient is the interface for the MQTT client
type MQTTClient struct {
	mu           sync.Mutex
	client       mqtt.Client
	config       MQTTConfig
	logger       *zap.Logger
	ctx          context.Context
	cancel       context.CancelFunc
	messageQueue *MessageQueue
}

func NewMQTTClient(config MQTTConfig, logger *zap.Logger) *MQTTClient {

	// Generate a new ClientID
	newClientId := generateClientID(config.ClientID)
	config.ClientID = newClientId

	return &MQTTClient{
		config: config,
	}
}

// generateClientID creates a random ClientID.
func generateClientID(baseID string) string {
	uuidPart := strings.Split(uuid.New().String(), "-")[0] // Extract the first part of the UUID
	return fmt.Sprintf("%s-%s", baseID, uuidPart)
}

func (m *MQTTClient) Connect(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.logger.Info("Connecting to MQTT broker", zap.String("broker", m.config.Broker), zap.Int("port", m.config.Port))
	m.logger.Debug("MQTT client configuration", zap.String("client_id", m.config.ClientID), zap.String("topic", m.config.Topic), zap.Uint8("qos", m.config.Qos), zap.Bool("clean_session", m.config.CleanSession), zap.Int("keep_alive", m.config.KeepAlive))

	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", m.config.Broker, m.config.Port))
	opts.SetClientID(m.config.ClientID)
	opts.SetCleanSession(m.config.CleanSession)
	opts.SetKeepAlive(time.Duration(m.config.KeepAlive) * time.Second)
	opts.SetUsername(m.config.Username)
	opts.SetPassword(m.config.Password)

	opts.OnConnect = m.onConnect
	opts.OnConnectionLost = m.onConnectionLost

	m.client = mqtt.NewClient(opts)

	m.ctx, m.cancel = context.WithCancel(ctx)

	// Initialize the message queue with a buffer of 100 messages and 5 workers
	m.messageQueue = NewMessageQueue(100, 5)

	m.messageQueue.StartProcessing(m.processMessage)

	token := m.client.Connect()
	if token.Wait() && token.Error() != nil {
		m.logger.Error("Error connecting to MQTT broker", zap.Error(token.Error()))
		return fmt.Errorf("error connecting to MQTT broker: %v", token.Error())
	}

	return nil
}

func (m *MQTTClient) Disconnect() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client != nil && m.client.IsConnected() {
		m.client.Disconnect(250)
		m.logger.Info("Disconnected from MQTT broker")
	}

	m.cancel()
}

func (m *MQTTClient) Subscribe() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client == nil || !m.client.IsConnected() {
		return fmt.Errorf("client is not connected")
	}

	token := m.client.Subscribe(m.config.Topic, byte(m.config.Qos), m.onMessage)
	token.Wait()
	m.logger.Info("Subscribed to topic", zap.String("topic", m.config.Topic))
	return token.Error()
}

func (m *MQTTClient) onConnect(client mqtt.Client) {
	m.logger.Info("Connected to MQTT broker")
}

func (m *MQTTClient) onConnectionLost(client mqtt.Client, err error) {
	m.logger.Error("Connection lost. Attempting to reconnect...", zap.Error(err))

	for {
		select {
		case <-m.ctx.Done():
			m.logger.Warn("Context canceled, stopping reconnection attempts")
			return
		default:
			if err := m.Connect(m.ctx); err != nil {
				m.logger.Warn("Reconnection failed. Retrying...", zap.Error(err))
			} else {
				m.logger.Info("Reconnected to MQTT broker")
				return
			}
			time.Sleep(5 * time.Second) // Wait before retrying
		}
	}
}

func (m *MQTTClient) onMessage(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()

	m.logger.Info("Received message", zap.String("topic", topic))
	m.logger.Debug("Message payload", zap.String("payload", string(msg.Payload())))

	// Add the received message to the message queue for asynchronous processing
	m.messageQueue.Enqueue(msg)
}

func (m *MQTTClient) processMessage(msg mqtt.Message) {
	// This will process messages asynchronously
	messageChannel <- msg
	// Add further processing logic here, such as writing to a database
}

func GetMessageChannel() chan mqtt.Message {
	return messageChannel
}
