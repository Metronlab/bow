package transformation

import "git.prod.metronlab.io/backend_libraries/go-bow/bow"

type Transformation interface {
	Apply(bow.Window) (*bow.Window, error)
}
