package btcexport

import (
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// s3Writer is a struct that provides an io.WriteCloser interface to an S3
// upload by piping writes to an s3manager.Uploader struct.
type s3Writer struct {
	writer  io.WriteCloser
	errChan chan error
}

// Assert that *s3Writer implements io.WriteCloser interface.
var _ io.WriteCloser = (*s3Writer)(nil)

// newS3Writer creates a new s3Writer for uploading an object to S3 via a
// WriteCloser interface. Data written will be chunked up and upload via the S3
// multipart upload API.
func newS3Writer(uploader *s3manager.Uploader, options *s3manager.UploadInput,
) (io.WriteCloser, error) {

	reader, writer := io.Pipe()

	input := *options
	input.Body = reader

	errChan := make(chan error)
	go func() {
		_, err := uploader.Upload(&input)
		errChan <- err
		close(errChan)
	}()

	return &s3Writer{writer: writer, errChan: errChan}, nil
}

// Write queues a byte slice to be uploaded to the S3 object location.
func (w *s3Writer) Write(p []byte) (int, error) {
	select {
	case err := <-w.errChan:
		if err != nil {
			return 0, fmt.Errorf("Upload exited with error: %v", err)
		} else {
			return 0, fmt.Errorf("Upload already exited")
		}
	default:
	}

	return w.writer.Write(p)
}

// Close signals the end of the data stream to the uploader and waits for the
// upload to complete.
func (w *s3Writer) Close() error {
	err := w.writer.Close()
	if err != nil {
		return err
	}

	// Wait for the upload to complete and return any errors.
	return <-w.errChan
}
