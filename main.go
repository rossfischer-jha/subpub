package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
)

func main() {
	var (
		projectID   = flag.String("project", "local-project", "GCP project ID")
		topic       = flag.String("topic", "", "Pub/Sub topic name (required)")
		msgType     = flag.String("type", "", "Message-Type attribute value")
		emulator    = flag.String("emulator", "localhost:9200", "Pub/Sub emulator address")
		institution = flag.String("institution", "", "InstitutionUniversalId attribute")
		wireID      = flag.String("wire-id", "", "WireId attribute")
	)

	flag.Parse()

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "Error: -topic is required")
		flag.Usage()
		os.Exit(1)
	}

	// Set emulator host
	os.Setenv("PUBSUB_EMULATOR_HOST", *emulator)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := pubsub.NewClient(ctx, *projectID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	// Read message payload from stdin
	payload, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read stdin: %v\n", err)
		os.Exit(1)
	}

	// Build attributes
	attrs := make(map[string]string)
	if *msgType != "" {
		attrs["Message-Type"] = *msgType
	}
	if *institution != "" {
		attrs["InstitutionUniversalId"] = *institution
	}
	if *wireID != "" {
		attrs["WireId"] = *wireID
	}

	// Publish message
	t := client.Topic(*topic)
	result := t.Publish(ctx, &pubsub.Message{
		Data:       payload,
		Attributes: attrs,
	})

	msgID, err := result.Get(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to publish: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Published message ID: %s\n", msgID)
	if len(attrs) > 0 {
		fmt.Printf("Attributes: %v\n", attrs)
	}
}
