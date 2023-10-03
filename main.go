package main

import (
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/streadway/amqp"
)

// Check if Queue Exist
func checkQueueExists(ch *amqp.Channel, queueName string) bool {
	// Declare the queue with passive set to true.
	fmt.Println("** Checking Queue Exists **")
	_, err := ch.QueueDeclare(
		queueName,    // name
		false,        // durable
		false,        // auto-delete
		false,        // exclusive
		false,        // no-wait
		amqp.Table{}, // arguments
	)
	if err != nil {
		// The queue does not exist.
		fmt.Println("Unable to create Queue @ CheckQueue")
		return false
	}

	// The queue exists.
	return true
}

// Create new Queue
func createQueue(ch *amqp.Channel, queueName string) error {
	// Declare the queue with passive set to false.
	_, err := ch.QueueDeclare(
		queueName,    // name
		false,        // durable
		false,        // auto-delete
		false,        // exclusive
		false,        // no-wait
		amqp.Table{}, // arguments
	)
	if err != nil {
		fmt.Println("Unable to create Queue @ CreateQueue")
		return err
	}

	return nil
}

// Ensure that queue exists.
func ensureQueueExists(ch *amqp.Channel, queueName string) error {
	// Check if the queue exists.
	exists := checkQueueExists(ch, queueName)
	if !exists {
		// Create the queue.
		err := createQueue(ch, queueName)
		if err != nil {
			fmt.Println("Unable to create Queue @ EnsureQueue")
			return err
		}
	}

	return nil
}

func consumeMessages(ch *amqp.Channel, queueName string) string {
	// Recieve the amqp.Delivery Channel
	msgChan, err := ch.Consume(
		queueName, // queue name
		"",        // consumer tag
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	// Declare []byte array to hold everything from queue
	var chunks [][]byte

	// Declare file URL variable
	var fileURL string

	for msg := range msgChan {
		// Add the chunk to the chunks.
		fmt.Println("** Combining chunks **")
		fmt.Println(msg.Body)
		chunks = append(chunks, msg.Body)
	}

	// Merge the chunks into a single video file.
	// TODO: Implement this.

	// Save the video file to the filesystem.
	// TODO: Implement this.

	// Return a String response to the user containing the URL of the video file.
	// TODO: Implement this.

	// Set fileURL variable
	fileURL = ""

	// Return fileURL
	fmt.Println("** Returning File URL **")
	return fileURL

}

// Global variables
var conn *amqp.Connection
var ch *amqp.Channel
var err error

func main() {
	// Create a new RabbitMQ connection.
	conn, err = amqp.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		fmt.Println("unable to connect to MQ")
		log.Fatal(err)
	}
	// Debug
	fmt.Println("** Connected to MQ Successfully **")

	// Create a new RabbitMQ channel.
	ch, err = conn.Channel()
	if err != nil {
		fmt.Println("unable to create a channel")
		log.Fatal(err)
	}
	// Debug
	fmt.Println("** Created MQ Channel Successfully **")

	// Declare a new RabbitMQ exchange of type `topic`.
	err = ch.ExchangeDeclare(
		"video-chunks", // name
		"topic",        // type
		true,           // durable
		false,          // auto-delete
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		fmt.Println("unable to create topic exchange")
		log.Fatal(err)
	}
	// Debug
	fmt.Println("** Created MQ Exchange Successfully **")

	r := mux.NewRouter()
	r.HandleFunc("/api/submit", submitChunk).Methods("POST")
	// Debug
	fmt.Println("** Server Listening **")
	http.ListenAndServe(":8080", r)
}

func submitChunk(w http.ResponseWriter, r *http.Request) {
	// Debug
	fmt.Println("** Handling Submit Chunk **")

	// Parse the multipart data
	fmt.Println("** Passing Multipart Data **")
	reader := multipart.NewReader(r.Body, "diff")
	// if err != nil {
	// 	fmt.Fprintf(w, "Error parsing multipart request: %v", err)
	// 	return
	// }

	if err != nil {
		fmt.Println("Error Passing Multipart")
		fmt.Println(err)
		return
	}

	// Declare chunkData
	var chunkData []byte

	fmt.Println("** Iterating Over multipart parts **")
	// Iterate over the parts in the multipart request
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("Error Getting Next Multipart")
			fmt.Println(err)
			return
		}

		// Get the body of the part
		_, err = part.Read(chunkData)
		if err != nil {
			fmt.Println("Unable to get Multipart request Body")
			fmt.Println(err)
			return
		}
	}

	// Get the session ID.
	sessionId := r.Header.Get("Session-ID")

	// Create a queue name for the session ID.
	queueName := fmt.Sprintf("queue-%s", sessionId)

	fmt.Println("** Ensuring Queue Exists **")
	// Ensure Queue Exists
	err = ensureQueueExists(ch, queueName)
	if err != nil {
		fmt.Println("** Failed to ensure Queue exists **")
	}

	fmt.Println("** Publishing Chunk to Exchange **")
	// Publish the chunk to the RabbitMQ exchange.
	err = ch.Publish(
		"video-chunks", // exchange name
		sessionId,      // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			Body: chunkData,
		})
	if err != nil {
		fmt.Println("Publish Error!!!")
		log.Println(err)
		return
	}

	if r.Header.Get("Complete") == "true" {
		// Consume & Process Queue Contents
		links := consumeMessages(ch, queueName)
		w.WriteHeader(200)
		fmt.Fprintln(w, links)
	} else {
		w.WriteHeader(200)
		fmt.Fprintln(w, "Video chunk acknowledged.")
	}

}
