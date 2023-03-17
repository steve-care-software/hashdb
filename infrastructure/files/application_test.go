package files

import (
	"bytes"
	"os"
	"reflect"
	"testing"

	infrastructure_database_files "github.com/steve-care-software/databases/infrastructure/files"
	"github.com/steve-care-software/libs/cryptography/hash"
)

func TestCreate_thenOpen_thenWrite_thenRead_Success(t *testing.T) {
	dirPath := "./test_files"
	dstExtension := "destination"
	bckExtension := "backup"
	readChunkSize := uint(1000000)
	defer func() {
		os.RemoveAll(dirPath)
	}()

	hashAdapter := hash.NewAdapter()
	database := infrastructure_database_files.NewApplication(dirPath, dstExtension, bckExtension, readChunkSize)
	hashDB := NewApplication(database)

	name := "my_name"
	err := database.New(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	pContext, err := database.Open(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	data := []byte("this is some data")
	pHash, err := hashAdapter.FromBytes(data)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	kind := uint(0)
	err = database.Write(*pContext, kind, *pHash, data)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	err = database.Commit(*pContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	retData, err := hashDB.Read(*pContext, kind, *pHash)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	if bytes.Compare(retData, data) != 0 {
		t.Errorf("the returned data is invalid")
		return
	}

	retContentKeys, err := database.ContentKeys(*pContext, kind)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	retContentKeysList := retContentKeys.List()
	if len(retContentKeysList) != 1 {
		t.Errorf("%d contentKeys od kinf (%d) were expected, %d returned", kind, 1, len(retContentKeysList))
		return
	}

	invalidKind := uint(2345234)
	_, err = database.ContentKeys(*pContext, invalidKind)
	if err == nil {
		t.Errorf("the error was expected to be valid, nil returned")
		return
	}

	retCommits, err := database.Commits(*pContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	list := retCommits.List()
	if len(list) != 1 {
		t.Errorf("%d commits were expected, %d returned", 1, len(list))
		return
	}

	retCommit, err := hashDB.Commit(*pContext, list[0].Hash())
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	if !reflect.DeepEqual(retCommit, list[0]) {
		t.Errorf("the returned commit is invalid")
		return
	}

	// erase by hashes:
	err = hashDB.EraseAll(*pContext, kind, []hash.Hash{
		*pHash,
	})

	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	// commit:
	err = database.Commit(*pContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	// read again, returns an error:
	_, err = hashDB.Read(*pContext, kind, *pHash)
	if err == nil {
		t.Errorf("the error was expected to be valid, nil returned")
		return
	}

	// insert again the resource we just deleted:
	err = database.Write(*pContext, kind, *pHash, data)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	err = database.Commit(*pContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	err = database.Close(*pContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	pSecondContext, err := database.Open(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	secondData := []byte("this is some second additional data")
	pSecondHash, err := hashAdapter.FromBytes(secondData)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	err = database.Write(*pSecondContext, kind, *pSecondHash, secondData)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	err = database.Commit(*pSecondContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	retSecondData, err := hashDB.Read(*pSecondContext, kind, *pSecondHash)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	err = database.Close(*pSecondContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	if bytes.Compare(retSecondData, secondData) != 0 {
		t.Errorf("the returned data is invalid")
		return
	}

	pThirdContext, err := database.Open(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	retFirstData, err := hashDB.Read(*pThirdContext, kind, *pHash)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	if bytes.Compare(retFirstData, data) != 0 {
		t.Errorf("the returned data is invalid")
		return
	}

	err = database.Close(*pThirdContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}
}

func TestCreate_New_Insert_Erase_Success(t *testing.T) {
	dirPath := "./test_files"
	dstExtension := "destination"
	bckExtension := "backup"
	readChunkSize := uint(1000000)
	defer func() {
		os.RemoveAll(dirPath)
	}()

	name := "my_name"
	database := infrastructure_database_files.NewApplication(dirPath, dstExtension, bckExtension, readChunkSize)
	hashDB := NewApplication(database)

	err := database.New(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	pContext, err := database.Open(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	defer database.Close(*pContext)
	data := []byte("this is some data")
	pHash, err := hash.NewAdapter().FromBytes(data)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	kind := uint(0)
	err = database.Write(*pContext, kind, *pHash, data)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	err = database.Commit(*pContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	// erase:
	err = hashDB.Erase(*pContext, kind, *pHash)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	err = database.Commit(*pContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}
}

func TestCreate_New_InsertResourceWithSameHashButDifferentKind_Success(t *testing.T) {
	dirPath := "./test_files"
	dstExtension := "destination"
	bckExtension := "backup"
	readChunkSize := uint(1000000)
	defer func() {
		os.RemoveAll(dirPath)
	}()

	name := "my_name"
	database := infrastructure_database_files.NewApplication(dirPath, dstExtension, bckExtension, readChunkSize)
	hashDB := NewApplication(database)

	err := database.New(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	pContext, err := database.Open(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	defer database.Close(*pContext)
	firstData := []byte("this is first data")
	pHash, err := hash.NewAdapter().FromBytes(firstData)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	firstKind := uint(0)
	err = database.Write(*pContext, firstKind, *pHash, firstData)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	secondKind := uint(1)
	secondData := []byte("this is the second data")
	err = database.Write(*pContext, secondKind, *pHash, secondData)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	// commit
	err = database.Commit(*pContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	// read first:
	retFirst, err := hashDB.Read(*pContext, firstKind, *pHash)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	if bytes.Compare(firstData, retFirst) != 0 {
		t.Errorf("the first data is invalid")
		return
	}

	// read second:
	retSecond, err := hashDB.Read(*pContext, secondKind, *pHash)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	if bytes.Compare(secondData, retSecond) != 0 {
		t.Errorf("the second data is invalid")
		return
	}
}

func TestCreate_Insert_thenDelete_thenInsert_SameKind_DifferentData_thenRead_Success(t *testing.T) {
	dirPath := "./test_files"
	dstExtension := "destination"
	bckExtension := "backup"
	readChunkSize := uint(1000000)
	defer func() {
		os.RemoveAll(dirPath)
	}()

	name := "my_name"
	database := infrastructure_database_files.NewApplication(dirPath, dstExtension, bckExtension, readChunkSize)
	hashDB := NewApplication(database)

	err := database.New(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	pContext, err := database.Open(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	defer database.Close(*pContext)
	firstData := []byte("this is first data")
	pHash, err := hash.NewAdapter().FromBytes(firstData)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	kind := uint(0)
	err = database.Write(*pContext, kind, *pHash, firstData)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	secondKind := uint(1)
	otherData := []byte("some other data yes!")
	err = database.Write(*pContext, secondKind, *pHash, otherData)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	// commit
	err = database.Commit(*pContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	// read:
	retData, err := hashDB.Read(*pContext, secondKind, *pHash)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	if bytes.Compare(otherData, retData) != 0 {
		t.Errorf("the data is invalid")
		return
	}

	// erase:
	err = hashDB.Erase(*pContext, kind, *pHash)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	err = hashDB.Erase(*pContext, secondKind, *pHash)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	// commit
	err = database.Commit(*pContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	// re-insert:
	err = database.Write(*pContext, kind, *pHash, firstData)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	err = database.Write(*pContext, secondKind, *pHash, otherData)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	// commit
	err = database.Commit(*pContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	// re-read:
	retSecondData, err := hashDB.Read(*pContext, secondKind, *pHash)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	if bytes.Compare(otherData, retSecondData) != 0 {
		t.Errorf("the data is invalid")
		return
	}
}
