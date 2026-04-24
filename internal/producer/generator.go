package producer

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
)

type GeneratorConfig struct {
	Interval    time.Duration
	BatchSize   int
	MaxSessions int
	Seed        int64
}

type Generator struct {
	cfg       GeneratorConfig
	publisher *Publisher
	logger    *slog.Logger

	mu       sync.Mutex
	running  bool
	cancel   context.CancelFunc
	done     chan struct{}
	sessions map[string]*syntheticSession
	rng      *rand.Rand
}

type syntheticSession struct {
	SessionID string
	UserID    string
	MovieID   string
	Device    DeviceType
	Step      int
	Progress  int
	NextAt    time.Time
}

func NewGenerator(cfg GeneratorConfig, publisher *Publisher, logger *slog.Logger) *Generator {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 750 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 2
	}
	if cfg.MaxSessions <= 0 {
		cfg.MaxSessions = 32
	}
	if cfg.Seed == 0 {
		cfg.Seed = time.Now().UnixNano()
	}

	return &Generator{
		cfg:       cfg,
		publisher: publisher,
		logger:    logger,
		sessions:  make(map[string]*syntheticSession),
		rng:       rand.New(rand.NewSource(cfg.Seed)),
	}
}

func (g *Generator) ApplyStartRequest(req GeneratorStartRequest) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if req.IntervalMS != nil {
		if *req.IntervalMS <= 0 {
			return fmt.Errorf("interval_ms must be positive")
		}
		g.cfg.Interval = time.Duration(*req.IntervalMS) * time.Millisecond
	}
	if req.BatchSize != nil {
		if *req.BatchSize <= 0 {
			return fmt.Errorf("batch_size must be positive")
		}
		g.cfg.BatchSize = *req.BatchSize
	}
	if req.MaxSessions != nil {
		if *req.MaxSessions <= 0 {
			return fmt.Errorf("max_sessions must be positive")
		}
		g.cfg.MaxSessions = *req.MaxSessions
	}
	if req.Seed != nil {
		g.cfg.Seed = *req.Seed
		g.rng = rand.New(rand.NewSource(*req.Seed))
	}
	return nil
}

func (g *Generator) Start(_ context.Context) (bool, error) {
	g.mu.Lock()
	if g.running {
		g.mu.Unlock()
		return false, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	g.running = true
	g.cancel = cancel
	g.done = make(chan struct{})
	g.sessions = make(map[string]*syntheticSession)
	if g.cfg.Seed != 0 {
		g.rng = rand.New(rand.NewSource(g.cfg.Seed))
	}
	g.mu.Unlock()

	go g.run(ctx)
	return true, nil
}

func (g *Generator) Stop() bool {
	g.mu.Lock()
	if !g.running {
		g.mu.Unlock()
		return false
	}
	cancel := g.cancel
	done := g.done
	g.running = false
	g.cancel = nil
	g.mu.Unlock()

	cancel()
	<-done
	return true
}

func (g *Generator) run(ctx context.Context) {
	defer func() {
		g.mu.Lock()
		g.running = false
		done := g.done
		g.done = nil
		g.cancel = nil
		g.mu.Unlock()
		if done != nil {
			close(done)
		}
	}()

	ticker := time.NewTicker(g.cfg.Interval)
	defer ticker.Stop()

	g.logger.Info("generator started",
		"interval_ms", g.cfg.Interval.Milliseconds(),
		"batch_size", g.cfg.BatchSize,
		"max_sessions", g.cfg.MaxSessions,
	)

	for {
		select {
		case <-ctx.Done():
			g.logger.Info("generator stopped")
			return
		case now := <-ticker.C:
			g.tick(ctx, now.UTC())
		}
	}
}

func (g *Generator) tick(ctx context.Context, now time.Time) {
	g.mu.Lock()
	available := g.cfg.MaxSessions - len(g.sessions)
	newCount := 0
	if available > 0 {
		newCount = 1 + g.rng.Intn(g.cfg.BatchSize)
		if newCount > available {
			newCount = available
		}
	}
	for i := 0; i < newCount; i++ {
		session := g.newSession(now)
		g.sessions[session.SessionID] = session
	}

	type pending struct {
		sessionID   string
		currentStep int
		event       MovieEvent
		nextStep    int
		nextProg    int
		nextAt      time.Time
		done        bool
	}

	var events []pending
	for _, session := range g.sessions {
		if now.Before(session.NextAt) {
			continue
		}
		event, nextStep, nextProgress, nextAt, done := g.nextEventForSession(session, now)
		events = append(events, pending{
			sessionID:   session.SessionID,
			currentStep: session.Step,
			event:       event,
			nextStep:    nextStep,
			nextProg:    nextProgress,
			nextAt:      nextAt,
			done:        done,
		})
	}
	g.mu.Unlock()

	for _, item := range events {
		pubCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := g.publisher.Publish(pubCtx, item.event)
		cancel()
		if err != nil {
			g.logger.Error("generator publish failed",
				"session_id", item.sessionID,
				"event_id", item.event.EventID,
				"event_type", item.event.EventType,
				"error", err,
			)
			continue
		}

		g.mu.Lock()
		if session, ok := g.sessions[item.sessionID]; ok && session.Step == item.currentStep {
			if item.done {
				delete(g.sessions, item.sessionID)
			} else {
				session.Step = item.nextStep
				session.Progress = item.nextProg
				session.NextAt = item.nextAt
			}
		}
		g.mu.Unlock()
	}

	if g.rng.Float64() < 0.35 {
		extra := g.randomBrowseEvent(now)
		pubCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := g.publisher.Publish(pubCtx, extra)
		cancel()
		if err != nil {
			g.logger.Error("generator supplemental event failed",
				"event_id", extra.EventID,
				"event_type", extra.EventType,
				"error", err,
			)
			return
		}
	}
}

func (g *Generator) newSession(now time.Time) *syntheticSession {
	userID := fmt.Sprintf("user-%04d", 1+g.rng.Intn(2000))
	movieID := fmt.Sprintf("movie-%04d", 1+g.rng.Intn(1500))
	device := []DeviceType{DeviceTypeMobile, DeviceTypeDesktop, DeviceTypeTV, DeviceTypeTablet}[g.rng.Intn(4)]
	return &syntheticSession{
		SessionID: uuid.NewString(),
		UserID:    userID,
		MovieID:   movieID,
		Device:    device,
		Step:      0,
		Progress:  0,
		NextAt:    now,
	}
}

func (g *Generator) nextEventForSession(session *syntheticSession, now time.Time) (MovieEvent, int, int, time.Time, bool) {
	base := MovieEvent{
		EventID:    uuid.NewString(),
		UserID:     session.UserID,
		MovieID:    session.MovieID,
		Timestamp:  TimestampMillis(now.UnixMilli()),
		DeviceType: session.Device,
		SessionID:  session.SessionID,
	}

	switch session.Step {
	case 0:
		base.EventType = EventTypeViewStarted
		base.ProgressSeconds = 0
		return base, 1, 0, now.Add(g.cfg.Interval), false
	case 1:
		session.Progress = 30 + g.rng.Intn(240)
		base.EventType = EventTypeViewPaused
		base.ProgressSeconds = session.Progress
		return base, 2, session.Progress, now.Add(g.cfg.Interval), false
	case 2:
		base.EventType = EventTypeViewResumed
		base.ProgressSeconds = session.Progress
		return base, 3, session.Progress, now.Add(g.cfg.Interval), false
	default:
		session.Progress += 60 + g.rng.Intn(420)
		base.EventType = EventTypeViewFinished
		base.ProgressSeconds = session.Progress
		return base, 4, session.Progress, now.Add(g.cfg.Interval), true
	}
}

func (g *Generator) randomBrowseEvent(now time.Time) MovieEvent {
	eventType := EventTypeLiked
	if g.rng.Float64() < 0.5 {
		eventType = EventTypeSearched
	}
	return MovieEvent{
		EventID:         uuid.NewString(),
		UserID:          fmt.Sprintf("user-%04d", 1+g.rng.Intn(2000)),
		MovieID:         fmt.Sprintf("movie-%04d", 1+g.rng.Intn(1500)),
		EventType:       eventType,
		Timestamp:       TimestampMillis(now.UnixMilli()),
		DeviceType:      []DeviceType{DeviceTypeMobile, DeviceTypeDesktop, DeviceTypeTV, DeviceTypeTablet}[g.rng.Intn(4)],
		SessionID:       uuid.NewString(),
		ProgressSeconds: 0,
	}
}
