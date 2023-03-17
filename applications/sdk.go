package applications

import (
	"github.com/steve-care-software/databases/domain/references"
	"github.com/steve-care-software/libs/cryptography/hash"
)

// Application represents the database application
type Application interface {
	List(context uint, kind uint) ([]hash.Hash, error)
	Read(context uint, kind uint, hash hash.Hash) ([]byte, error)
	ReadAll(context uint, kind uint, hashes []hash.Hash) ([][]byte, error)
	Erase(context uint, kind uint, hash hash.Hash) error
	EraseAll(context uint, kind uint, hashes []hash.Hash) error
	Commit(context uint, hash hash.Hash) (references.Commit, error)
}
