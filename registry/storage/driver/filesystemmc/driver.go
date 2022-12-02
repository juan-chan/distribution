package filesystemmc

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	dcontext "github.com/juan-chan/distribution/context"
	"github.com/juan-chan/distribution/registry/grpc"
	storagedriver "github.com/juan-chan/distribution/registry/storage/driver"
	"github.com/juan-chan/distribution/registry/storage/driver/base"
	"github.com/juan-chan/distribution/registry/storage/driver/factory"
	"github.com/juan-chan/distribution/registry/storage/manager"
)

const (
	driverName                   = "filesystemmc"
	defaultRootDirectory         = "/var/lib/registry"
	defaultMaxThreads            = uint64(100)
	defaultStorageManagerAddress = ""

	// minThreads is the minimum value for the maxthreads configuration
	// parameter. If the driver's parameters are less than this we set
	// the parameters to minThreads
	minThreads = uint64(25)
)

// DriverParameters represents all configuration options available for the
// filesystem driver
type DriverParameters struct {
	RootDirectory         string
	MaxThreads            uint64
	StorageManagerAddress string
}

func init() {
	factory.Register(driverName, &filesystemDriverFactory{})
}

// filesystemDriverFactory implements the factory.StorageDriverFactory interface
type filesystemDriverFactory struct{}

func (factory *filesystemDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type driver struct {
	rootDirectory         string
	storageManagerAddress string

	// grpcConnPool is a gRPC client connection pool.
	grpcConnPool *grpc.ConnPool
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by a local
// filesystem. All provided paths will be subpaths of the RootDirectory.
type Driver struct {
	baseEmbed
}

// FromParameters constructs a new Driver with a given parameters map
// Optional Parameters:
// - rootdirectory
// - maxthreads
func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	params, err := fromParametersImpl(parameters)
	if err != nil || params == nil {
		return nil, err
	}
	return New(*params), nil
}

func fromParametersImpl(parameters map[string]interface{}) (*DriverParameters, error) {
	var (
		err                   error
		maxThreads            = defaultMaxThreads
		rootDirectory         = defaultRootDirectory
		storageManagerAddress = defaultStorageManagerAddress
	)

	if parameters != nil {
		if rootDir, ok := parameters["rootdirectory"]; ok {
			rootDirectory = fmt.Sprint(rootDir)
		}

		maxThreads, err = base.GetLimitFromParameter(parameters["maxthreads"], minThreads, defaultMaxThreads)
		if err != nil {
			return nil, fmt.Errorf("maxthreads config error: %s", err.Error())
		}

		if ad, ok := parameters["smaddress"]; ok {
			storageManagerAddress = fmt.Sprint(ad)
		}
	}

	params := &DriverParameters{
		RootDirectory:         rootDirectory,
		MaxThreads:            maxThreads,
		StorageManagerAddress: storageManagerAddress,
	}
	return params, nil
}

// New constructs a new Driver with a given rootDirectory
func New(params DriverParameters) *Driver {
	fsDriver := &driver{
		rootDirectory:         params.RootDirectory,
		storageManagerAddress: params.StorageManagerAddress,

		grpcConnPool: grpc.NewConnPool(),
	}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: base.NewRegulator(fsDriver, params.MaxThreads),
			},
		},
	}
}

// Implement the storagedriver.StorageDriver interface

func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	rc, err := d.Reader(ctx, path, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	p, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}

	return p, nil
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, subPath string, contents []byte) error {
	writer, err := d.Writer(ctx, subPath, false)
	if err != nil {
		return err
	}
	defer writer.Close()
	_, err = io.Copy(writer, bytes.NewReader(contents))
	if err != nil {
		writer.Cancel()
		return err
	}
	return writer.Commit()
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {

	fullPath, err := d.fullPath(path, ctx)

	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(fullPath, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, storagedriver.PathNotFoundError{Path: path}
		}

		return nil, err
	}

	seekPos, err := file.Seek(offset, io.SeekStart)
	if err != nil {
		file.Close()
		return nil, err
	} else if seekPos < offset {
		file.Close()
		return nil, storagedriver.InvalidOffsetError{Path: path, Offset: offset}
	}

	return file, nil
}

func (d *driver) Writer(ctx context.Context, subPath string, append bool) (storagedriver.FileWriter, error) {
	fullPath, err := d.fullPath(subPath, ctx)

	if err != nil {
		return nil, err
	}

	parentDir := path.Dir(fullPath)
	if err := os.MkdirAll(parentDir, 0777); err != nil {
		return nil, err
	}

	fp, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	var offset int64

	if !append {
		err := fp.Truncate(0)
		if err != nil {
			fp.Close()
			return nil, err
		}
	} else {
		n, err := fp.Seek(0, io.SeekEnd)
		if err != nil {
			fp.Close()
			return nil, err
		}
		offset = n
	}

	return newFileWriter(fp, offset), nil
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, subPath string) (storagedriver.FileInfo, error) {
	fullPath, err := d.fullPath(subPath, ctx)

	if err != nil {
		return nil, err
	}

	fi, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, storagedriver.PathNotFoundError{Path: subPath}
		}

		return nil, err
	}

	return fileInfo{
		path:     subPath,
		FileInfo: fi,
	}, nil
}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *driver) List(ctx context.Context, subPath string) ([]string, error) {
	fullPath, err := d.fullPath(subPath, ctx)

	if err != nil {
		return nil, err
	}

	dir, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, storagedriver.PathNotFoundError{Path: subPath}
		}
		return nil, err
	}

	defer dir.Close()

	fileNames, err := dir.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(fileNames))
	for _, fileName := range fileNames {
		keys = append(keys, path.Join(subPath, fileName))
	}

	return keys, nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	source, err := d.fullPath(sourcePath, ctx)

	if err != nil {
		return err
	}

	dest, err := d.fullPath(destPath, ctx)

	if err != nil {
		return err
	}

	if _, err := os.Stat(source); os.IsNotExist(err) {
		return storagedriver.PathNotFoundError{Path: sourcePath}
	}

	if err := os.MkdirAll(path.Dir(dest), 0755); err != nil {
		return err
	}

	err = os.Rename(source, dest)
	return err
}

func (d *driver) BackupAndDeleteWithHost(ctx context.Context, host, path string) error {
	return d.Move(ctx, path, fmt.Sprintf("backup/%s", path))
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, subPath string) error {

	fullPath, err := d.fullPath(subPath, ctx)

	if err != nil {
		return err
	}

	_, err = os.Stat(fullPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	} else if err != nil {
		return storagedriver.PathNotFoundError{Path: subPath}
	}

	err = os.RemoveAll(fullPath)
	return err
}

// DeleteWithHost recursively deletes all objects stored at "path" and its subPaths with coding host.
func (d *driver) DeleteWithHost(ctx context.Context, host, path string) error {
	fullPath, err := d.fullPathWithHost(ctx, host, path)
	if err != nil {
		return err
	}

	_, err = os.Stat(fullPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	} else if err != nil {
		return storagedriver.PathNotFoundError{Path: path}
	}

	err = os.RemoveAll(fullPath)
	return err
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	return "", storagedriver.ErrUnsupportedMethod{}
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file
func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {
	return storagedriver.WalkFallback(ctx, d, path, f)
}

func stringMap(vs []string, f func(string) string) []string {
	vsm := make([]string, len(vs))
	for i, v := range vs {
		vsm[i] = f(v)
	}
	return vsm
}

func find(vs []string, f func(string) bool) (string, bool) {
	for _, v := range vs {
		ok := f(v)
		if ok {
			return v, ok
		}
	}

	return "", false
}

// try to get _manifests / _layers / _uploads lastIndex
func lastIndexOfImageContent(subPath string) int {
	MANIFESTS := "_manifests"
	LAYERS := "_layers"
	UPLOADS := "_uploads"

	target, ok := find([]string{MANIFESTS, LAYERS, UPLOADS}, func(s string) bool {

		return strings.LastIndex(subPath, s) != -1
	})

	if ok {
		return strings.LastIndex(subPath, target)
	}

	return -1

}

func getPrefix(ctx context.Context) string {
	host := dcontext.GetStringValue(ctx, "http.request.host")

	chunks := strings.Split(host, ".")

	return chunks[0]
}

// fullPath returns the absolute path of a key within the Driver's storage.
func (d *driver) fullPath(subPath string, ctx context.Context) (string, error) {

	if d.storageManagerAddress != "" {
		storagePath, err := manager.GetStoragePath(
			d.grpcConnPool,
			d.storageManagerAddress,
			dcontext.GetStringValue(ctx, "http.request.host"),
			subPath)
		if err != nil {
			return "", fmt.Errorf("get storage path failed: %v", err)
		}
		return path.Join(d.rootDirectory, storagePath), nil
	}

	prefix := getPrefix(ctx)

	return path.Join(d.rootDirectory, prefix, subPath), nil
}

func (d *driver) fullPathWithHost(ctx context.Context, host, subPath string) (string, error) {
	prefix := getPrefix(ctx)
	if d.storageManagerAddress == "" {
		return path.Join(d.rootDirectory, prefix, subPath), nil
	}
	if host == "" {
		return d.fullPath(subPath, ctx)
	}

	storagePath, err := manager.GetStoragePath(d.grpcConnPool, d.storageManagerAddress, host, subPath)
	if err != nil {
		return "", fmt.Errorf("failed to get storage path: %v", err)
	}
	return path.Join(d.rootDirectory, storagePath), nil
}

type fileInfo struct {
	os.FileInfo
	path string
}

var _ storagedriver.FileInfo = fileInfo{}

// Path provides the full path of the target of this file info.
func (fi fileInfo) Path() string {
	return fi.path
}

// Size returns current length in bytes of the file. The return value can
// be used to write to the end of the file at path. The value is
// meaningless if IsDir returns true.
func (fi fileInfo) Size() int64 {
	if fi.IsDir() {
		return 0
	}

	return fi.FileInfo.Size()
}

// ModTime returns the modification time for the file. For backends that
// don't have a modification time, the creation time should be returned.
func (fi fileInfo) ModTime() time.Time {
	return fi.FileInfo.ModTime()
}

// IsDir returns true if the path is a directory.
func (fi fileInfo) IsDir() bool {
	return fi.FileInfo.IsDir()
}

type fileWriter struct {
	file      *os.File
	size      int64
	bw        *bufio.Writer
	closed    bool
	committed bool
	cancelled bool
}

func newFileWriter(file *os.File, size int64) *fileWriter {
	return &fileWriter{
		file: file,
		size: size,
		bw:   bufio.NewWriter(file),
	}
}

func (fw *fileWriter) Write(p []byte) (int, error) {
	if fw.closed {
		return 0, fmt.Errorf("already closed")
	} else if fw.committed {
		return 0, fmt.Errorf("already committed")
	} else if fw.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}
	n, err := fw.bw.Write(p)
	fw.size += int64(n)
	return n, err
}

func (fw *fileWriter) Size() int64 {
	return fw.size
}

func (fw *fileWriter) Close() error {
	if fw.closed {
		return fmt.Errorf("already closed")
	}

	if err := fw.bw.Flush(); err != nil {
		return err
	}

	if err := fw.file.Sync(); err != nil {
		return err
	}

	if err := fw.file.Close(); err != nil {
		return err
	}
	fw.closed = true
	return nil
}

func (fw *fileWriter) Cancel() error {
	if fw.closed {
		return fmt.Errorf("already closed")
	}

	fw.cancelled = true
	fw.file.Close()
	return os.Remove(fw.file.Name())
}

func (fw *fileWriter) Commit() error {
	if fw.closed {
		return fmt.Errorf("already closed")
	} else if fw.committed {
		return fmt.Errorf("already committed")
	} else if fw.cancelled {
		return fmt.Errorf("already cancelled")
	}

	if err := fw.bw.Flush(); err != nil {
		return err
	}

	if err := fw.file.Sync(); err != nil {
		return err
	}

	fw.committed = true
	return nil
}
