// Command go-amqp-perf exercises github.com/Azure/go-amqp (throughput samples).
// Point go.mod at a fork via replace to try a different build; same import path.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/go-amqp"
)

const streamOffsetSpec = "rabbitmq:stream-offset-spec"

func main() {
	amqpURL := flag.String("amqp", envOr("AMQP_URL", "amqp://guest:guest@127.0.0.1:5672/"), "Broker URL")
	queue := flag.String("queue", "/queues/test", "Queue for send / settlement benchmarks")
	stream := flag.String("stream", envOr("AMQP_STREAM_SOURCE", "/queues/test"), "RabbitMQ stream address (publish + consume)")
	dur := flag.Duration("duration", 10*time.Second, "Duration for each throughput sample")
	fill := flag.Duration("fill-stream", 5*time.Second, "Publish duration to seed the stream before consume benchmarks")
	settlementDepth := flag.Int("settlement-queue-depth", 4, "SenderOptions.SettlementQueueDepth for settlement-channel benchmark (fork builds only)")
	flag.Parse()

	ctx := context.Background()

	fmt.Println("=== go-amqp throughput ===")
	fmt.Printf("lib=%s\n", linkedGoAMQPDesc())
	fmt.Printf("broker=%s queue=%s stream=%s sample=%s fill-stream=%s settlement-depth=%d\n\n",
		*amqpURL, *queue, *stream, dur, fill, *settlementDepth)

	if err := fillStream(ctx, *amqpURL, *stream, *fill); err != nil {
		log.Fatalf("fill stream: %v", err)
	}

	send, err := benchSend(ctx, *amqpURL, *queue, *dur)
	if err != nil {
		log.Fatalf("send: %v", err)
	}
	recv, err := benchSendReceipt(ctx, *amqpURL, *queue, *dur)
	if err != nil {
		log.Fatalf("send+receipt: %v", err)
	}

	settle, settleErr := benchSettlementChannel(ctx, *amqpURL, *queue, *dur, *settlementDepth)
	if settleErr != nil && !errors.Is(settleErr, errSettlementUnsupported) {
		log.Fatalf("settlement channel: %v", settleErr)
	}

	fmt.Printf("%-36s %12s\n", "benchmark", "msg/s (or noted)")
	fmt.Println(strings.Repeat("-", 50))
	fmt.Printf("%-36s %12.0f\n", "Send (unsettled)", send)
	fmt.Printf("%-36s %12.0f\n", "Send + receipt.Wait", recv)
	if errors.Is(settleErr, errSettlementUnsupported) {
		fmt.Printf("%-36s %12s\n", "Send + Settlements chan (settled/s)", "n/a (not in this build)")
	} else {
		fmt.Printf("%-36s %12.0f\n", "Send + Settlements chan (settled/s)", settle)
	}
	fmt.Println()

	credits := []int32{0, 1, 4, 8, 32}
	fmt.Printf("Consume stream %q (offset=first), msg/s:\n", *stream)
	fmt.Printf("%-14s %12s\n", "recv credit", "msg/s")
	fmt.Println(strings.Repeat("-", 28))
	for _, c := range credits {
		rate, err := benchConsume(ctx, *amqpURL, *stream, *dur, c)
		if err != nil {
			log.Fatalf("consume credit=%d: %v", c, err)
		}
		label := fmt.Sprintf("%d", c)
		if c == 0 {
			label = "0 (default)"
		}
		fmt.Printf("%-14s %12.0f\n", label, rate)
	}
	fmt.Println()
}

func linkedGoAMQPDesc() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "github.com/Azure/go-amqp (?)"
	}
	for _, m := range info.Deps {
		if m.Path == "github.com/Azure/go-amqp" {
			if m.Replace != nil {
				return fmt.Sprintf("%s %s => %s %s", m.Path, m.Version, m.Replace.Path, m.Replace.Version)
			}
			return m.Path + " " + m.Version
		}
	}
	return "github.com/Azure/go-amqp (not in dependency list)"
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func isExpectedTermination(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	return strings.Contains(err.Error(), "transfer-limit-exceeded")
}

func fillStream(ctx context.Context, url, stream string, fill time.Duration) error {
	conn, err := amqp.Dial(ctx, url, &amqp.ConnOptions{SASLType: amqp.SASLTypePlain("guest", "guest")})
	if err != nil {
		return err
	}
	defer conn.Close()
	sess, err := conn.NewSession(ctx, nil)
	if err != nil {
		return err
	}
	sender, err := sess.NewSender(ctx, stream, nil)
	if err != nil {
		return err
	}
	defer sender.Close(context.Background())
	msg := amqp.NewMessage([]byte("fill"))
	deadline := time.Now().Add(fill)
	for time.Now().Before(deadline) {
		if err := sender.Send(ctx, msg, nil); err != nil {
			return err
		}
	}
	return nil
}

func recvOpts(credit int32) *amqp.ReceiverOptions {
	f := []amqp.LinkFilter{amqp.NewLinkFilter(streamOffsetSpec, 0, "first")}
	if credit <= 0 {
		return &amqp.ReceiverOptions{Filters: f}
	}
	return &amqp.ReceiverOptions{Credit: credit, Filters: f}
}

func benchSend(ctx context.Context, url, queue string, d time.Duration) (float64, error) {
	conn, err := amqp.Dial(ctx, url, &amqp.ConnOptions{SASLType: amqp.SASLTypePlain("guest", "guest")})
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	sess, err := conn.NewSession(ctx, nil)
	if err != nil {
		return 0, err
	}
	sender, err := sess.NewSender(ctx, queue, nil)
	if err != nil {
		return 0, err
	}
	defer sender.Close(context.Background())
	msg := amqp.NewMessage([]byte("hello"))
	return measureSend(ctx, d, func(c context.Context) error { return sender.Send(c, msg, nil) })
}

func benchSendReceipt(ctx context.Context, url, queue string, d time.Duration) (float64, error) {
	conn, err := amqp.Dial(ctx, url, &amqp.ConnOptions{SASLType: amqp.SASLTypePlain("guest", "guest")})
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	sess, err := conn.NewSession(ctx, nil)
	if err != nil {
		return 0, err
	}
	sender, err := sess.NewSender(ctx, queue, nil)
	if err != nil {
		return 0, err
	}
	defer sender.Close(context.Background())
	msg := amqp.NewMessage([]byte("hello"))
	return measureSend(ctx, d, func(c context.Context) error {
		r, err := sender.SendWithReceipt(c, msg, nil)
		if err != nil {
			return err
		}
		_, err = r.Wait(c)
		return err
	})
}

func benchConsume(ctx context.Context, url, stream string, d time.Duration, credit int32) (float64, error) {
	conn, err := amqp.Dial(ctx, url, &amqp.ConnOptions{SASLType: amqp.SASLTypePlain("guest", "guest")})
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	sess, err := conn.NewSession(ctx, nil)
	if err != nil {
		return 0, err
	}
	recv, err := sess.NewReceiver(ctx, stream, recvOpts(credit))
	if err != nil {
		return 0, err
	}
	defer recv.Close(context.Background())
	sender, err := sess.NewSender(ctx, stream, nil)
	if err != nil {
		return 0, err
	}
	defer sender.Close(context.Background())
	return runConsumeLoop(ctx, d,
		func(c context.Context, b []byte) error { return sender.Send(c, amqp.NewMessage(b), nil) },
		func(c context.Context) (*amqp.Message, error) { return recv.Receive(c, nil) },
		func(c context.Context, m *amqp.Message) error { return recv.AcceptMessage(c, m) },
	)
}

func runConsumeLoop[R any](
	ctx context.Context,
	d time.Duration,
	send func(context.Context, []byte) error,
	recv func(context.Context) (R, error),
	accept func(context.Context, R) error,
) (float64, error) {
	sub, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	var n atomic.Uint64
	start := time.Now()

	go func() {
		for sub.Err() == nil {
			_ = send(sub, []byte("x"))
		}
	}()

	for sub.Err() == nil {
		m, err := recv(sub)
		if err != nil {
			if isExpectedTermination(err) {
				break
			}
			return 0, err
		}
		if err := accept(sub, m); err != nil {
			if isExpectedTermination(err) {
				break
			}
			return 0, err
		}
		n.Add(1)
	}
	elapsed := time.Since(start).Seconds()
	if elapsed < 1e-9 {
		elapsed = 1e-9
	}
	return float64(n.Load()) / elapsed, nil
}

func measureSend(ctx context.Context, d time.Duration, op func(context.Context) error) (float64, error) {
	sub, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	var n atomic.Uint64
	start := time.Now()
	for sub.Err() == nil {
		if err := op(sub); err != nil {
			if isExpectedTermination(err) {
				break
			}
			return 0, err
		}
		n.Add(1)
	}
	elapsed := time.Since(start).Seconds()
	if elapsed < 1e-9 {
		elapsed = 1e-9
	}
	return float64(n.Load()) / elapsed, nil
}
