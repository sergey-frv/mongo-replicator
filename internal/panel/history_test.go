package panel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// seriesOf — series, в которую по порядку добавлены vals
func seriesOf(vals ...float64) series {
	var s series
	for _, v := range vals {
		s.push(v)
	}
	return s
}

// numbers — 1, 2, …, n
func numbers(n int) []float64 {
	vals := make([]float64, n)
	for i := range vals {
		vals[i] = float64(i + 1)
	}
	return vals
}

func TestSeriesEmpty(t *testing.T) {
	s := seriesOf()

	assert.Empty(t, s.values())
	assert.Equal(t, 0, s.count)
}

func TestSeriesShort(t *testing.T) {
	s := seriesOf(5, 0, 7)

	assert.Equal(t, []float64{5, 0, 7}, s.values())
	assert.Equal(t, 3, s.count)
}

func TestSeriesFull(t *testing.T) {
	s := seriesOf(numbers(60)...)

	vals := s.values()
	assert.Len(t, vals, 60)
	assert.Equal(t, []float64{1, 2}, vals[:2], "старые — первыми")
	assert.Equal(t, 60.0, vals[59], "новое — последним")
	assert.Equal(t, 60, s.count)
}

func TestSeriesOverflow(t *testing.T) {
	s := seriesOf(numbers(62)...)

	vals := s.values()
	assert.Len(t, vals, 60)
	assert.Equal(t, []float64{3, 4}, vals[:2], "1 и 2 вытеснены")
	assert.Equal(t, 62.0, vals[59], "новое — последним")
	assert.Equal(t, 62, s.count)
}

func TestCatchUpEnter(t *testing.T) {
	cases := []struct {
		name   string
		lag    time.Duration
		active bool
	}{
		{"ниже минуты", 59 * time.Second, false},
		{"минута", time.Minute, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var c catchUp

			c.update(tc.lag, clock(12, 0, 0))

			assert.Equal(t, tc.active, c.active)
		})
	}
}

func TestCatchUpHoldsBetweenThresholds(t *testing.T) {
	var c catchUp
	c.update(2*time.Minute, clock(12, 0, 0))

	c.update(30*time.Second, clock(12, 0, 10))

	assert.True(t, c.active)
}

func TestCatchUpExit(t *testing.T) {
	var c catchUp
	c.update(2*time.Minute, clock(12, 0, 0))

	c.update(9*time.Second, clock(12, 0, 10))

	assert.False(t, c.active)
	assert.Empty(t, c.points)
}

func TestCatchUpReenter(t *testing.T) {
	var c catchUp
	c.update(2*time.Minute, clock(12, 0, 0))
	c.update(9*time.Second, clock(12, 0, 10))

	c.update(90*time.Second, clock(12, 0, 20))

	assert.True(t, c.active)
	assert.Equal(t, 90*time.Second, c.start)
	assert.Equal(t, []lagPoint{{clock(12, 0, 20), 90 * time.Second}}, c.points)
}

func TestCatchUpLagRiseRaisesStart(t *testing.T) {
	var c catchUp
	c.update(time.Minute, clock(12, 0, 0))

	c.update(3*time.Minute, clock(12, 0, 10))

	assert.Equal(t, 3*time.Minute, c.start)
}

func TestCatchUpUnknownLag(t *testing.T) {
	var c catchUp
	c.update(2*time.Minute, clock(12, 0, 0))

	c.update(-1, clock(12, 0, 10))

	assert.True(t, c.active)
	assert.Equal(t, []lagPoint{{clock(12, 0, 0), 2 * time.Minute}}, c.points)
}

func TestCatchUpSameLagAddsNoPoint(t *testing.T) {
	var c catchUp
	c.update(2*time.Minute, clock(12, 0, 0))

	c.update(2*time.Minute, clock(12, 0, 1))
	c.update(2*time.Minute, clock(12, 0, 2))

	assert.Equal(t, []lagPoint{{clock(12, 0, 0), 2 * time.Minute}}, c.points)
}

func TestCatchUpWindowKeepsLast30s(t *testing.T) {
	var c catchUp
	c.update(3*time.Minute, clock(12, 0, 0))
	c.update(170*time.Second, clock(12, 0, 10))
	c.update(160*time.Second, clock(12, 0, 20))

	c.update(150*time.Second, clock(12, 0, 40))

	assert.Equal(t, []lagPoint{
		{clock(12, 0, 10), 170 * time.Second},
		{clock(12, 0, 20), 160 * time.Second},
		{clock(12, 0, 40), 150 * time.Second},
	}, c.points, "точка 12:00:00 старше 30 с")
}

func TestCatchUpWindowKeepsTwoPoints(t *testing.T) {
	var c catchUp
	c.update(3*time.Minute, clock(12, 0, 0))
	c.update(170*time.Second, clock(12, 0, 10))

	c.update(170*time.Second, clock(12, 1, 0))

	assert.Len(t, c.points, 2)
}

func TestCatchUpFrac(t *testing.T) {
	var c catchUp
	c.update(4*time.Minute, clock(12, 0, 0))

	c.update(time.Minute, clock(12, 0, 10))

	assert.InDelta(t, 0.75, c.frac(), 1e-9)
}

func TestCatchUpEstimate(t *testing.T) {
	var c catchUp
	c.update(2*time.Minute, clock(12, 0, 0))

	c.update(100*time.Second, clock(12, 0, 10))

	assert.Equal(t, "~50с до 0", c.estimate(), "лаг падает на 2 с в секунду")
}

func TestCatchUpEstimateOnePoint(t *testing.T) {
	var c catchUp

	c.update(2*time.Minute, clock(12, 0, 0))

	assert.Empty(t, c.estimate())
}

func TestCatchUpEstimateLagRising(t *testing.T) {
	var c catchUp
	c.update(2*time.Minute, clock(12, 0, 0))

	c.update(130*time.Second, clock(12, 0, 10))

	assert.Equal(t, "лаг не снижается", c.estimate())
}

func TestCatchUpEstimateBeyond99Days(t *testing.T) {
	var c catchUp
	c.update(100*24*time.Hour, clock(12, 0, 0))

	c.update(100*24*time.Hour-time.Second, clock(12, 0, 10))

	assert.Equal(t, "лаг не снижается", c.estimate(), "оценка ~1000 дней")
}
