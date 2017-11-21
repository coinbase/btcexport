package btcexport

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// RotatingWriter is used to write to a backing writer that can be rotated
// periodically. This is used to write to different files with an approximate
// size limit and start a new file when that size limit is exceeded.
type RotatingWriter struct {
	openWriter func() (io.WriteCloser, error)

	size   int
	writer io.WriteCloser
}

// NewRotatingWriter constructs a new RotatingWriter which uses the openWriter
// parameter function to generate a new backing writer each time it is rotated.
func NewRotatingWriter(openWriter func() (io.WriteCloser, error),
) (*RotatingWriter, error) {

	w := &RotatingWriter{openWriter: openWriter}
	err := w.RotateWriter()
	if err != nil {
		return nil, err
	}
	return w, nil
}

// Write writes to the backing writer and records the number of bytes written.
func (w *RotatingWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	w.size += n
	return n, err
}

// BytesWritten returns the total number of bytes written to the current backing
// writer. This is reset each time the writer is rotated.
func (w *RotatingWriter) BytesWritten() int {
	return w.size
}

// RotateWriter closes the current backing writer and opens a new one, resetting
// the count of bytes written.
func (w *RotatingWriter) RotateWriter() error {
	if w.writer != nil {
		err := w.Close()
		if err != nil {
			return err
		}
	}

	writer, err := w.openWriter()
	if err != nil {
		return err
	}

	w.writer = writer
	return nil
}

// Close closes the current backing writer.
func (w *RotatingWriter) Close() error {
	err := w.writer.Close()
	if err != nil {
		return err
	}

	w.size = 0
	w.writer = nil
	return nil
}

// WriterFactory is a function that can be used to create new RotatingWriters
// with unique file names.
type WriterFactory func(filename string, indexPtr *uint32) (*RotatingWriter, error)

// RotatingFileWriter returns a factory for creating new RotatingWriters with a
// file output destination. File names are generated sequentially using a shared
// incrementing index.
func RotatingFileWriter(dir string) WriterFactory {
	return func(filename string, indexPtr *uint32) (*RotatingWriter, error) {
		return NewRotatingWriter(func() (io.WriteCloser, error) {
			index := atomic.AddUint32(indexPtr, 1)
			filePath := filepath.Join(dir, fmt.Sprintf(filename, index))
			return os.Create(filePath)
		})
	}
}

// RotatingS3Writer returns a factory for creating new RotatingWriters with an
// S3 object output destination. Object keys are generated sequentially using a
// shared incrementing index.
func RotatingS3Writer(uploader *s3manager.Uploader,
	options *s3manager.UploadInput) WriterFactory {

	return func(filename string, indexPtr *uint32) (*RotatingWriter, error) {
		return NewRotatingWriter(func() (io.WriteCloser, error) {
			index := atomic.AddUint32(indexPtr, 1)
			key := *options.Key + fmt.Sprintf(filename, index)

			optionsOverride := *options
			optionsOverride.Key = &key
			return newS3Writer(uploader, &optionsOverride)
		})
	}
}
