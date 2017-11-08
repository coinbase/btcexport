package btcexport

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
)

var (
	errExceededWriterCapacity error = errors.New("write would exceed file size limit")
)

// cappedWriter is used to write to a backing writer up to a certain data limit.
// In order to ensure this across writes, cappedWriter tracks the total amount
// written.
type cappedWriter struct {
	size     int
	capacity int
	writer   io.Writer
}

// Write writes to the backing writer unless the write would exceed the limit.
// If there is not sufficient capacity, this does not perform the write and
// returns an error instead.
func (w *cappedWriter) Write(p []byte) (int, error) {
	if w.size+len(p) > w.capacity {
		return 0, errExceededWriterCapacity
	}
	n, err := w.writer.Write(p)
	w.size += n
	return n, err
}

type FileWriter struct {
	filePathTemplate string
	indexPtr         *uint32
	capacity         int

	file   *os.File
	writer *cappedWriter
}

func NewFileWriter(dir string, filename string, indexPtr *uint32, capacity int,
) (*FileWriter, error) {

	writer := &FileWriter{
		filePathTemplate: filepath.Join(dir, filename),
		indexPtr:         indexPtr,
		capacity:         capacity,
	}
	err := writer.initNewFile()
	if err != nil {
		return nil, err
	}
	return writer, nil
}

func (w *FileWriter) initNewFile() error {
	index := atomic.AddUint32(w.indexPtr, 1)
	filePath := fmt.Sprintf(w.filePathTemplate, index)
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}

	w.file = file
	w.writer = &cappedWriter{
		capacity: w.capacity,
		writer:   file,
	}
	return nil
}

func (w *FileWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)

	if err == errExceededWriterCapacity {
		err = w.Close()
		if err != nil {
			return n, err
		}

		err = w.initNewFile()
		if err != nil {
			return n, err
		}

		n, err = w.writer.Write(p)
	}

	return n, err
}

func (w *FileWriter) Close() error {
	return w.file.Close()
}
