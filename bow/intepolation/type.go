package intepolation

import "errors"

type Interpolation int

const (
	Linear = iota
	StepPrevious
)

var (
	ErrUnknownInterpolation = errors.New("unknown interpolation")
)

func Validate(interpolation Interpolation) error {
	switch interpolation {
	case Linear, StepPrevious:
		return nil
	default:
		return ErrUnknownInterpolation
	}
}
