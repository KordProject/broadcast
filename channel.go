package broadcast

import (
	"context"
	"sync"
)

// A generic Event structure to make identifying the event publisher simpler for efficient distribution
type MapChannelEvent[T comparable, T2 any] struct {
	// The publisher of the event
	Publisher T
	// The event that needs to the distributed
	Event T2
}

// A map of channels that can create subscribing channels from a given publisher and distribute events to the all of the subscribers
type BroadcastChannel[T comparable, T2 any] struct {
	// mutex to prevent cross channel issues
	mutex sync.Mutex
	// inbound events channel for sources to send events to
	events chan *MapChannelEvent[T, T2]
	// map of maps to track subscriptions of publishers
	//
	// takes the form `{publisherX: {subscriberA: subscriptionChA...}...}`
	subscriptions map[T]map[T]chan T2
	// Tracks the last event value for each publisher and sends that to new subscribers if it missed events
	cache map[T]T2
}

// Create a new broadcast channel and initialize internal maps and channels
func NewBroadcastChannel[T comparable, T2 any]() *BroadcastChannel[T, T2] {
	return &BroadcastChannel[T, T2]{
		// create a buffered inbound event channel to prevent blocking publishers
		events:        make(chan *MapChannelEvent[T, T2], 10),
		subscriptions: make(map[T]map[T]chan T2),
		cache:         make(map[T]T2),
	}
}

// Publish an event to a publisher's subscribers
func (b *BroadcastChannel[T, T2]) SendEvent(publisher T, event T2) {
	// basically just map the inbound event to the internal struct for simplicity
	b.events <- &MapChannelEvent[T, T2]{
		Event:     event,
		Publisher: publisher,
	}
}

// Returns a write-only channel for publishers to send messages to directly for convenience
func (b *BroadcastChannel[T, T2]) PublisherChannel() chan<- *MapChannelEvent[T, T2] {
	return b.events
}

// Main function to handle sending events to subscribers. This can handle a dynamic number of subscribers because it doesn't care about sending events to subscribers until an event is published. If a subscriber is falling behind, discard the old event so we don't block other subscribers.
func (b *BroadcastChannel[T, T2]) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// allow a user to stop this execution
			b.Stop()
			return
		case msg, open := <-b.events:
			if !open {
				// if the events channel was closed, we must be shutting down
				return
			}
			b.mutex.Lock()
			// save the event to the cache
			if b.cache != nil {
				b.cache[msg.Publisher] = msg.Event
			}

			// broadcast the published event
			// This is where we read the subscriptions, hence the RLock to prevent modifying it while we are broadcasting an event. Any new subscriptions will block until we are done broadcasting and then the new subscriber will get whatever event was last published by their publisher.
			var wg sync.WaitGroup
			if subscribers, ok := b.subscriptions[msg.Publisher]; ok {
				for _, ch := range subscribers {
					wg.Add(1)
					go func(c chan T2) {
						defer wg.Done()
						if len(c) > 0 {
							// we know the subscriber channel can only handle 1 buffered event at a time so if the subscriber is falling behind, discard the old event so we don't block ourselves
							select {
							case <-c:
							default:
							}
						}
						c <- msg.Event
					}(ch)
				}
			}
			wg.Wait()
			b.mutex.Unlock()
		}
	}
}

// Shutdown this broadcaster and clear the memory
func (b *BroadcastChannel[T, T2]) Stop() {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	defer close(b.events)

	for publisher, subscribers := range b.subscriptions {
		for subscriber, subscription := range subscribers {
			close(subscription)
			delete(subscribers, subscriber)
		}
		delete(b.subscriptions, publisher)
		delete(b.cache, publisher)
	}

	b.cache = nil
}

// Allow a subscriber to specify a publisher that it wants to subscribe to and receive a channel that events will be forwarded to
func (b *BroadcastChannel[T, T2]) Subscribe(publisher T, subscriber T) <-chan T2 {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// if the publisher doesn't exist, create it
	if _, ok := b.subscriptions[publisher]; !ok {
		b.subscriptions[publisher] = make(map[T]chan T2)
	}

	var sub chan T2
	var ok bool
	// if the subscriber doesn't exist, create it
	if sub, ok = b.subscriptions[publisher][subscriber]; !ok {
		// make it slightly buffered so that we can send events and not block ourselves before the subscriber can start handling stuff
		b.subscriptions[publisher][subscriber] = make(chan T2, 1)
		sub = b.subscriptions[publisher][subscriber]
	}

	// if there is a cached event for this publisher, send it to the subscriber
	if event, ok := b.cache[publisher]; ok {
		sub <- event
	}

	return sub
}

// Allow a subscriber to unsubscribe from a publisher
func (b *BroadcastChannel[T, T2]) Unsubscribe(publisher T, subscriber T) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// remove the subscribing destination from the registry
	if subscribers, ok := b.subscriptions[publisher]; ok {
		// if the subscription exists, close the channel and delete it
		if sub, ok := subscribers[subscriber]; ok {
			close(sub)
			delete(subscribers, subscriber)
		}
		// if this publisher has no subscribers left, delete it
		if len(subscribers) == 0 {
			delete(b.subscriptions, publisher)
			delete(b.cache, publisher)
		}
	}
}
