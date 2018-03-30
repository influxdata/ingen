package ingen

import "strings"

// ErrorList is a simple error aggregator to return multiple errors as one.
type ErrorList []error

func (el ErrorList) Error() string {
	b := new(strings.Builder)
	for _, err := range el {
		b.WriteString(err.Error())
		b.WriteByte('\n')
	}
	return b.String()
}

func NewErrorList(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	return ErrorList(errs)
}
