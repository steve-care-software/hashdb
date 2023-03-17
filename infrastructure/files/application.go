package files

import (
	"errors"
	"fmt"

	databases "github.com/steve-care-software/databases/applications"
	"github.com/steve-care-software/databases/domain/references"
	hashdb "github.com/steve-care-software/hashdb/applications"
	"github.com/steve-care-software/libs/cryptography/hash"
)

type application struct {
	pointerDB databases.Application
}

func createApplication(
	pointerDB databases.Application,
) hashdb.Application {
	out := application{
		pointerDB: pointerDB,
	}

	return &out
}

// List returns the hashes by kind
func (app *application) List(context uint, kind uint) ([]hash.Hash, error) {
	keys, err := app.pointerDB.ContentKeysByKind(context, kind)
	if err != nil {
		return nil, err
	}

	hashes := []hash.Hash{}
	list := keys.List()
	for _, oneContentKey := range list {
		hashes = append(hashes, oneContentKey.Hash())
	}

	return hashes, nil
}

// Read reads content by hash
func (app *application) Read(context uint, kind uint, hash hash.Hash) ([]byte, error) {
	contentKey, err := app.retrieveActiveContentKeyByHash(context, kind, hash)
	if err != nil {
		return nil, err
	}

	return app.pointerDB.Read(context, contentKey.Content())
}

// ReadAll reads content by hashes
func (app *application) ReadAll(context uint, kind uint, hashes []hash.Hash) ([][]byte, error) {
	output := [][]byte{}
	for _, oneHash := range hashes {
		content, err := app.Read(context, kind, oneHash)
		if err != nil {
			return nil, err
		}

		output = append(output, content)
	}

	return output, nil
}

// Erase erases by hash
func (app *application) Erase(context uint, kind uint, hash hash.Hash) error {
	// retrieve the content key:
	contentKey, err := app.retrieveActiveContentKeyByHash(context, kind, hash)
	if err != nil {
		return err
	}

	return app.pointerDB.Erase(context, contentKey)
}

// EraseAll erases by hashes
func (app *application) EraseAll(context uint, kind uint, hashes []hash.Hash) error {
	for _, oneHash := range hashes {
		err := app.Erase(context, kind, oneHash)
		if err != nil {
			return err
		}
	}

	return nil
}

func (app *application) retrieveActiveContentKeyByHash(context uint, kind uint, hash hash.Hash) (references.ContentKey, error) {
	contentKeys, err := app.pointerDB.ContentKeysByKind(context, kind)
	if err != nil {
		return nil, err
	}

	if contentKeys == nil {
		str := fmt.Sprintf("the resource (kind: %d, hash: %s) could not be fetched because it does not exists", kind, hash.String())
		return nil, errors.New(str)
	}

	return contentKeys.Fetch(kind, hash)
}
