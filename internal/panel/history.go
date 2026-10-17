package panel

import "time"

// seriesLen — сколько точек хранит series: минута при тике раз в секунду
const seriesLen = 60

// series — последние seriesLen значений для графика. count — сколько значений добавлено
// всего: по нему колонки графика привязываются к абсолютному номеру точки и сдвигаются
// вместе с данными, а не пересчитываются на каждом тике.
type series struct {
	vals  []float64
	count int
}

func (s *series) push(v float64) {
	if len(s.vals) == seriesLen {
		copy(s.vals, s.vals[1:])
		s.vals = s.vals[:seriesLen-1]
	}
	s.vals = append(s.vals, v)
	s.count++
}

// values — значения от старых к новым, не больше seriesLen
func (s *series) values() []float64 { return s.vals }

const (
	catchUpEnter  = time.Minute      // лаг, с которого начинается догон
	catchUpExit   = 10 * time.Second // лаг, ниже которого догон закончен
	catchUpWindow = 30 * time.Second // за сколько хранятся точки для оценки
	catchUpMaxETA = 99 * 24 * time.Hour
)

type lagPoint struct {
	at  time.Time
	lag time.Duration
}

// catchUp — режим догона: лаг вырос до catchUpEnter и ещё не упал ниже catchUpExit.
// Между порогами режим не меняется, чтобы полоса не мигала. Точки добавляются только при
// изменении лага: репликатор обновляет его раз в 10 с, и точки каждую секунду дали бы
// ступеньки и прыгающую оценку.
type catchUp struct {
	active bool
	start  time.Duration // наибольший лаг с начала догона — 0% полосы
	points []lagPoint    // за catchUpWindow, но не меньше двух последних
}

// update учитывает лаг из снимка в at; lag < 0 (неизвестен) режим не меняет.
func (c *catchUp) update(lag time.Duration, at time.Time) {
	switch {
	case lag < 0:
		return
	case !c.active:
		if lag >= catchUpEnter {
			c.active, c.start, c.points = true, lag, []lagPoint{{at, lag}}
		}
		return
	case lag < catchUpExit:
		c.active, c.points = false, nil
		return
	}
	c.start = max(c.start, lag)
	if c.points[len(c.points)-1].lag != lag {
		c.points = append(c.points, lagPoint{at, lag})
	}
	for len(c.points) > 2 && at.Sub(c.points[0].at) > catchUpWindow {
		c.points = c.points[1:]
	}
}

// frac — доля пройденного догона по последней точке, 0–1
func (c *catchUp) frac() float64 {
	if !c.active || c.start <= 0 {
		return 0
	}
	f := 1 - float64(c.points[len(c.points)-1].lag)/float64(c.start)
	return min(max(f, 0), 1)
}

// estimate — оценка времени до нулевого лага по первой и последней точке окна:
// «~1м0с до 0», «лаг не снижается» или пусто, пока точек меньше двух.
func (c *catchUp) estimate() string {
	if !c.active || len(c.points) < 2 {
		return ""
	}
	first, last := c.points[0], c.points[len(c.points)-1]
	dt := last.at.Sub(first.at).Seconds()
	if dt <= 0 {
		return ""
	}
	drop := (first.lag - last.lag).Seconds() / dt // секунд лага за секунду
	if drop <= 0 {
		return "лаг не снижается"
	}
	eta := last.lag.Seconds() / drop
	if eta > catchUpMaxETA.Seconds() {
		return "лаг не снижается"
	}
	return "~" + fmtDuration(time.Duration(eta*float64(time.Second))) + " до 0"
}
