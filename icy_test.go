package main

import (
	"testing"
	"time"
)

func TestCalcSleep(t *testing.T) {
	now := time.Now()
	nowFn = func() time.Time { return now }
	defer func() { nowFn = time.Now }()

	for _, tc := range []struct {
		Name       string
		StartTime  time.Time
		ServedTime time.Duration
		WantSleep  time.Duration
	}{
		{
			Name:       "Behind by 30 seconds",
			StartTime:  now.Add(-1 * time.Minute),
			ServedTime: 30 * time.Second,
			WantSleep:  1 * time.Nanosecond,
		},
		{
			Name:       "On time",
			StartTime:  now.Add(-1 * time.Minute),
			ServedTime: 1*time.Minute + 30*time.Second,
			WantSleep:  1 * time.Nanosecond,
		},
		{
			Name:       "40 seconds ahead",
			StartTime:  now.Add(-1 * time.Minute),
			ServedTime: 1*time.Minute + 40*time.Second,
			WantSleep:  10 * time.Second,
		},
		{
			Name:       "50 seconds ahead",
			StartTime:  now.Add(-1 * time.Hour),
			ServedTime: 1*time.Hour + 50*time.Second,
			WantSleep:  20 * time.Second,
		},
		{
			Name:       "Behind by 1 minute",
			StartTime:  now.Add(-1 * time.Hour),
			ServedTime: 1*time.Hour - 1*time.Minute - 30*time.Second,
			WantSleep:  1 * time.Nanosecond,
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			got := calculateIcySleep(tc.StartTime, tc.ServedTime)
			if got != tc.WantSleep {
				t.Errorf("want sleep %s, got: %s", tc.WantSleep, got)
			}
		})
	}
}
