package broadcast_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/KordProject/broadcast"
)

func TestCreateBroadcast(t *testing.T) {
	val := 123
	b := broadcast.NewBroadcastChannel[string, int]()
	go b.Run(context.Background())
	s := b.Subscribe("foo", "bar")
	b.SendEvent("foo", val)
	a := <-s
	if a != val {
		t.Errorf("incorrect value returned from channel (wanted %d, got %d)", val, a)
	}
	b.Stop()
}

func TestBroadcastContext(t *testing.T) {
	val := 123
	b := broadcast.NewBroadcastChannel[string, int]()
	ctx, cancelFunc := context.WithCancel(context.Background())
	go b.Run(ctx)
	s := b.Subscribe("foo", "bar")
	b.SendEvent("foo", val)
	a := <-s
	if a != val {
		t.Errorf("incorrect value returned from channel (wanted %d, got %d)", val, a)
	}
	cancelFunc()
}

func TestBroadcastBlocking(t *testing.T) {
	val := 123
	pub := "foo"
	sub := "bar"
	b := broadcast.NewBroadcastChannel[string, int]()
	go b.Run(context.Background())
	defer b.Stop()
	s := b.Subscribe(pub, sub)
	b.SendEvent(pub, val-2)
	b.SendEvent(pub, val-1)
	b.SendEvent(pub, val)
	// just wait long enough to pretend we are falling behind to the first values are overwritten
	time.Sleep(time.Millisecond)
	a := <-s
	if a != val {
		t.Errorf("incorrect value returned from channel (wanted %d, got %d)", val, a)
	}
	b.Unsubscribe(pub, sub)
	v, ok := <-s
	if ok {
		t.Errorf("channel should be closed but got %d", v)
	}
}

func TestBroadcastCache(t *testing.T) {
	val := 123
	pub := "foo"
	sub := "bar"
	sub2 := "baz"
	b := broadcast.NewBroadcastChannel[string, int]()
	go b.Run(context.Background())
	b.Subscribe(pub, sub)
	b.SendEvent(pub, val-2)
	b.SendEvent(pub, val-1)
	b.SendEvent(pub, val)

	// just wait long enough to pretend we are falling behind to the first values are overwritten
	time.Sleep(time.Millisecond)
	s := b.Subscribe(pub, sub2)
	a := <-s
	if a != val {
		t.Errorf("incorrect value returned from channel (wanted %d, got %d)", val, a)
	}
	b.Unsubscribe(pub, sub2)
	v, ok := <-s
	if ok {
		t.Errorf("channel should be closed but got %d", v)
	}
	b.Stop()
}

func TestBroadcastDirectSend(t *testing.T) {
	val := 123
	pub := "foo"
	sub := "bar"
	b := broadcast.NewBroadcastChannel[string, int]()
	go b.Run(context.Background())
	s := b.Subscribe(pub, sub)
	e := b.PublisherChannel()
	e <- &broadcast.MapChannelEvent[string, int]{
		Publisher: pub,
		Event:     val - 2,
	}
	e <- &broadcast.MapChannelEvent[string, int]{
		Publisher: pub,
		Event:     val - 1,
	}
	e <- &broadcast.MapChannelEvent[string, int]{
		Publisher: pub,
		Event:     val,
	}
	// just wait long enough to pretend we are falling behind to the first values are overwritten
	time.Sleep(time.Millisecond)
	a := <-s
	if a != val {
		t.Errorf("incorrect value returned from channel (wanted %d, got %d)", val, a)
	}
	b.Stop()
	v, ok := <-s
	if ok {
		t.Errorf("channel should be closed but got %d", v)
	}
}

func BenchmarkNewBroadcast(b *testing.B) {
	var broadcaster *broadcast.BroadcastChannel[int, int]
	for i := 0; i < b.N; i++ {
		broadcaster = broadcast.NewBroadcastChannel[int, int]()
	}
	broadcaster.Stop()
}

func BenchmarkBroadcastSubscribe(b *testing.B) {
	tests := []struct {
		publishers  int
		subscribers int
	}{
		{1, 1},
		{1, 10},
		{1, 1024},
		{10, 100},
		{100, 10},
		{1024, 1024},
		{4096, 1024},
	}

	for _, t := range tests {
		b.Run(fmt.Sprintf("%d-Publishers/%d-Subscribers", t.publishers, t.subscribers), func(b *testing.B) {
			broadcaster := broadcast.NewBroadcastChannel[int, int]()
			go broadcaster.Run(context.Background())

			subscriptions := make(map[int]map[int]<-chan int)
			for i := 0; i < t.publishers; i++ {
				subscriptions[i] = make(map[int]<-chan int)
			}

			for i := 0; i < t.subscribers; i++ {
				publisher := rand.Intn(t.publishers)
				subscriptions[publisher][i] = broadcaster.Subscribe(publisher, i)
			}

			for _, s := range subscriptions {
				for _, c := range s {
					go func(c <-chan int) {
						<-c
					}(c)
				}
			}

			b.ResetTimer()

			var value int
			for i := 0; i < b.N; i++ {
				publisher := rand.Intn(t.publishers)
				value = rand.Intn(1000000)
				broadcaster.SendEvent(publisher, value)
			}
			time.Sleep(time.Millisecond)
			broadcaster.Stop()
		})
	}
}
