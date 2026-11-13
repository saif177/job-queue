package queue

import "time"

// bucket is a token bucket: it holds up to burst tokens and refills at rate
// tokens per second. Callers hold Queue.mu.
type bucket struct {
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func (b *bucket) refill(now time.Time) {
	if b.last.IsZero() {
		b.last = now
		return
	}
	if el := now.Sub(b.last).Seconds(); el > 0 {
		b.tokens += el * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = now
	}
}

func (b *bucket) ready(now time.Time) bool {
	b.refill(now)
	return b.tokens >= 1
}

// WithRateLimit limits how fast jobs of the given type are started: at most
// perSecond jobs per second on average, with bursts of up to burst jobs.
// Jobs over the limit stay pending (they do not fail or consume retries) and
// never block ready jobs of other types. Only typed jobs (EnqueueJob) can be
// limited. Non-positive perSecond or burst values are ignored.
func WithRateLimit(typ string, perSecond float64, burst int) Option {
	return func(q *Queue) {
		if typ == "" || perSecond <= 0 || burst < 1 {
			return
		}
		if q.limits == nil {
			q.limits = make(map[string]*bucket)
		}
		q.limits[typ] = &bucket{rate: perSecond, burst: float64(burst), tokens: float64(burst)}
	}
}

// rateOK reports whether a job of typ may start now without consuming a token.
func (q *Queue) rateOK(typ string, now time.Time) bool {
	b, ok := q.limits[typ]
	return !ok || b.ready(now)
}

// takeToken consumes a token for typ if the type is rate limited.
func (q *Queue) takeToken(typ string) {
	if b, ok := q.limits[typ]; ok {
		b.tokens--
	}
}
