package operations

import (
	"context"
	"io"

	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/fs/accounting"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// state for a multi-thread copy
type multiThreadCopyState struct {
	ctx      context.Context
	partSize int64
	size     int64
	wc       fs.WriterAtCloser
	src      fs.Object
	acc      *accounting.Account
	streams  int
}

// Copy a single stream into place
func (mc *multiThreadCopyState) copyStream(stream int) (err error) {
	defer func() {
		if err != nil {
			fs.Debugf(mc.src, "multi-thread copy: stream %d/%d failed: %v", stream+1, mc.streams, err)
		}
	}()
	start := int64(stream) * mc.partSize
	if start >= mc.size {
		return nil
	}
	end := start + mc.partSize
	if end > mc.size {
		end = mc.size
	}

	fs.Debugf(mc.src, "multi-thread copy: stream %d/%d (%d-%d) size %v starting", stream+1, mc.streams, start, end, fs.SizeSuffix(end-start))

	rc, err := newReOpen(mc.src, nil, &fs.RangeOption{Start: start, End: end - 1}, fs.Config.LowLevelRetries)
	if err != nil {
		return errors.Wrap(err, "multpart copy: failed to open source")
	}
	defer fs.CheckClose(rc, &err)

	// Copy the data
	buf := make([]byte, 32*1024)
	offset := start
	for {
		// Check if context cancelled and exit if so
		if mc.ctx.Err() != nil {
			return mc.ctx.Err()
		}
		nr, er := rc.Read(buf)
		if nr > 0 {
			err = mc.acc.AccountRead(nr)
			if err != nil {
				return errors.Wrap(err, "multpart copy: accounting failed")
			}
			nw, ew := mc.wc.WriteAt(buf[0:nr], offset)
			if nw > 0 {
				offset += int64(nw)
			}
			if ew != nil {
				return errors.Wrap(ew, "multpart copy: write failed")
			}
			if nr != nw {
				return errors.Wrap(io.ErrShortWrite, "multpart copy")
			}
		}
		if er != nil {
			if er != io.EOF {
				return errors.Wrap(er, "multpart copy: read failed")
			}
			break
		}
	}

	if offset != end {
		return errors.Errorf("multpart copy: wrote %d bytes but expected to write %d", offset-start, end-start)
	}

	fs.Debugf(mc.src, "multi-thread copy: stream %d/%d (%d-%d) size %v finished", stream+1, mc.streams, start, end, fs.SizeSuffix(end-start))
	return nil
}

// Copy src to (f, remote) using streams download threads and the OpenWriterAt feature
func multiThreadCopy(f fs.Fs, remote string, src fs.Object, streams int) (newDst fs.Object, err error) {
	openWriterAt := f.Features().OpenWriterAt
	if openWriterAt == nil {
		return nil, errors.New("multi-thread copy: OpenWriterAt not supported")
	}

	const (
		chunkSize     = 64 << 10
		chunkSizeMask = chunkSize - 1
	)

	g, ctx := errgroup.WithContext(context.Background())
	mc := &multiThreadCopyState{
		ctx:  ctx,
		size: src.Size(),
		src:  src,
	}
	partSize := mc.size / int64(streams)
	if mc.size < 0 {
		return nil, errors.New("multi-thread copy: can't copy unknown sized file")
	}
	// Round partition size up so partSize * streams >= size
	if (mc.size % int64(streams)) != 0 {
		partSize++
	}
	// round partSize up to nearest chunkSize boundary
	mc.partSize = (partSize + chunkSizeMask) &^ chunkSizeMask
	// recalculate number of streams
	streams = int(mc.size / mc.partSize)
	// round streams up so partSize * streams >= size
	if (mc.size % mc.partSize) != 0 {
		streams++
	}
	mc.streams = streams

	// Make accounting
	mc.acc = accounting.NewAccount(nil, src)
	defer fs.CheckClose(mc.acc, &err)

	// create write file handle
	mc.wc, err = openWriterAt(remote, mc.size)
	if err != nil {
		return nil, errors.Wrap(err, "multpart copy: failed to open destination")
	}
	defer fs.CheckClose(mc.wc, &err)

	fs.Debugf(src, "Starting multi-thread copy with %d parts of size %v", streams, fs.SizeSuffix(partSize))
	for stream := 0; stream < streams; stream++ {
		stream := stream
		g.Go(func() (err error) {
			return mc.copyStream(stream)
		})
	}
	err = g.Wait()
	if err != nil {
		return nil, err
	}

	obj, err := f.NewObject(remote)
	if err != nil {
		return nil, errors.Wrap(err, "multi-thread copy: failed to find object after copy")
	}

	err = obj.SetModTime(src.ModTime())
	switch err {
	case nil, fs.ErrorCantSetModTime, fs.ErrorCantSetModTimeWithoutDelete:
	default:
		return nil, errors.Wrap(err, "multi-thread copy: failed to set modification time")
	}

	fs.Debugf(src, "Finished multi-thread copy with %d parts of size %v", streams, fs.SizeSuffix(partSize))
	return obj, nil
}
