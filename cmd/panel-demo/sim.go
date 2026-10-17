package main

import (
	"sync"
	"time"

	"github.com/sergey-frv/mongo-replicator/internal/replicator"
	"github.com/sergey-frv/mongo-replicator/internal/statefile"
	"github.com/sergey-frv/mongo-replicator/internal/traffic"
)

// Тексты WARN сценария.
const (
	retryWarn = "ошибка репликации, повтор (демо)"
	collWarn  = "urlshrinker.links: коллекция создана без опций источника, проверьте вручную (демо)"
	retryErr  = "bulk write в urlshrinker.links: connection reset by peer (демо)"
)

// sim — мок-статус: запуск, догон с лагом больше суток, медленный хвост догона
// (с 30-й секунды лаг идёт от 2 мин к 0 за 30 с), ровный поток с 60-й секунды,
// повтор на 100–110-й секунде. Значения — функции времени с запуска, без случайности.
type sim struct {
	mu       sync.Mutex
	st       replicator.Status
	lastWarn time.Time // последний периодический WARN; нулевое — не было
}

func newSim(start time.Time) *sim {
	return &sim{st: replicator.Status{
		DB: "urlshrinker", DstDB: "urlshrinker_v2", Started: start,
		Lag: -1, State: replicator.StateStarting,
	}}
}

func (s *sim) status() replicator.Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st
}

func (s *sim) stopped() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.st.State = replicator.StateStopped
}

// step продвигает сценарий к now (шаг — 200 мс) и возвращает тексты WARN. Сам не
// логирует: лог панели отправляет строку в цикл событий bubbletea и ждёт его, а
// цикл на тике ждёт s.mu в status — запись под s.mu была бы взаимоблокировкой.
func (s *sim) step(now time.Time) (warns []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	up := now.Sub(s.st.Started)
	switch {
	case up < 2*time.Second:
		return nil // «запуск»: поток ещё не открыт
	case up >= 100*time.Second && up < 110*time.Second:
		if s.st.State != replicator.StateRetry {
			s.st.State, s.st.Retry, s.st.LastErr = replicator.StateRetry, 1, retryErr
			warns = append(warns, retryWarn)
		}
		return warns
	}
	s.st.State, s.st.Retry, s.st.LastErr = replicator.StateRunning, 0, ""

	// догон: лаг 28 ч тает к 30-й секунде, хвост — от 2 мин к 0 к 60-й секунде
	// (полоса, оценка и выход ниже 10 с), затем ровный поток с лагом 0–1,5 с
	sec := int(up / time.Second)
	catchUp, tail := 28*time.Hour-up*3360, (60*time.Second-up)*4
	var lag time.Duration
	var rate float64
	switch {
	case catchUp > tail:
		lag, rate = catchUp, 12000
	case tail > 0:
		lag, rate = tail, 300
	default:
		lag, rate = time.Duration(sec%16)*100*time.Millisecond, 5+float64(sec%21)
	}
	// операций за шаг 200 мс; n — то же для байт трафика: из float, не uint64(ops) —
	// на такой конверсии range-анализ gosec падает («not an Int»)
	perStep := rate / 5
	ops, n := int(perStep)+1, uint64(perStep)+1
	s.st.Applied += int64(ops)
	s.st.Rate = rate
	s.st.Lag, s.st.AppliedAt = lag, now.Add(-lag)

	ins, del := ops*6/10, ops*3/10
	s.st.LastBatch = &replicator.BatchInfo{
		At:    now,
		Ops:   replicator.OpCounts{Ins: ins, Upd: ops - ins - del, Del: del},
		Colls: []string{"PrivateUrls", "Counters", "PublicUrls"},
	}
	s.st.Traffic = s.st.Traffic.Add(traffic.Info{SrcIn: n * 510, SrcOut: 60, DstIn: n * 22, DstOut: n * 170})
	s.st.Machine = &statefile.Machine{
		At: now.Truncate(time.Minute), Processes: 3, Active: 2,
		Traffic: s.st.Traffic.Add(traffic.Info{SrcIn: 12 << 30, SrcOut: 300 << 20, DstIn: 200 << 20, DstOut: 11 << 30}),
	}
	if up > 20*time.Second && s.st.LastDDL == nil {
		s.st.LastDDL = &replicator.DDLInfo{At: now, Op: "createIndexes", NS: "urlshrinker.PublicUrls", Details: "slug_1"}
	}
	if up > 10*time.Second && now.Sub(s.lastWarn) >= 25*time.Second {
		s.lastWarn = now
		warns = append(warns, collWarn)
	}
	return warns
}
