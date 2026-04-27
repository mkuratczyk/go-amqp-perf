package main

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/Azure/go-amqp"
)

// errSettlementUnsupported means the linked go-amqp build has no SenderOptions.Settlements
// (e.g. upstream Azure release). Swap dependency in go.mod to a fork that adds it.
var errSettlementUnsupported = errors.New("go-amqp: SenderOptions.Settlements not available in this build")

func senderSupportsSettlementChannel() bool {
	t := reflect.TypeFor[amqp.SenderOptions]()
	_, okS := t.FieldByName("Settlements")
	_, okD := t.FieldByName("SettlementQueueDepth")
	return okS && okD
}

func benchSettlementChannel(ctx context.Context, url, queue string, d time.Duration, depth int) (float64, error) {
	if depth <= 0 {
		return 0, nil
	}
	if !senderSupportsSettlementChannel() {
		return 0, errSettlementUnsupported
	}

	optsType := reflect.TypeFor[amqp.SenderOptions]()
	opts := reflect.New(optsType).Elem()
	opts.FieldByName("SettlementQueueDepth").SetInt(int64(depth))

	settlementsField, _ := optsType.FieldByName("Settlements")
	chType := settlementsField.Type // e.g. chan<- amqp.Settlement
	elem := chType.Elem()
	bi := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, elem), depth)
	opts.FieldByName("Settlements").Set(bi.Convert(chType))

	senderOpts := opts.Addr().Interface().(*amqp.SenderOptions)

	conn, err := amqp.Dial(ctx, url, &amqp.ConnOptions{SASLType: amqp.SASLTypePlain("guest", "guest")})
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	sess, err := conn.NewSession(ctx, nil)
	if err != nil {
		return 0, err
	}
	sender, err := sess.NewSender(ctx, queue, senderOpts)
	if err != nil {
		return 0, err
	}
	defer sender.Close(context.Background())
	msg := amqp.NewMessage([]byte("hello"))

	sub, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	var sent, settled atomic.Uint64
	start := time.Now()
	go func() {
		for {
			_, ok := bi.Recv()
			if !ok {
				return
			}
			settled.Add(1)
		}
	}()
	for sub.Err() == nil {
		_, err := sender.SendWithReceipt(sub, msg, nil)
		if err != nil {
			if isExpectedTermination(err) {
				break
			}
			return 0, err
		}
		sent.Add(1)
	}
	total := sent.Load()
	deadline := time.Now().Add(5 * time.Second)
	for settled.Load() < total && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	elapsed := time.Since(start).Seconds()
	if elapsed < 1e-9 {
		elapsed = 1e-9
	}
	return float64(settled.Load()) / elapsed, nil
}
