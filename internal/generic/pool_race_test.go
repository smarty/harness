//go:build race

package generic

// The race-detector build of sync.Pool deliberately drops Puts at random
// to widen race coverage, so recycling is not always observable under -race.
const raceDetectorEnabled = true
