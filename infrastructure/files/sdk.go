package files

import (
	databases "github.com/steve-care-software/databases/applications"
	"github.com/steve-care-software/hashdb/applications"
)

// NewApplication creates a new application instance
func NewApplication(
	pointerDB databases.Application,
) applications.Application {
	return createApplication(pointerDB)
}
