package cos

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	stdlog "log"
	"net"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/docker/distribution/registry/grpc"
	"github.com/docker/distribution/registry/storage/manager"

	"github.com/sirupsen/logrus"
	"github.com/tencentyun/cos-go-sdk-v5"

	dcontext "github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/cos/cci"
	"github.com/docker/distribution/registry/storage/driver/cos/cdn"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

const (
	driverName                   = "cos"
	listMax                      = 1000
	minChunkSize                 = 1 << 20
	defaultChunkSize             = 2 * minChunkSize
	defaultRootDirectory         = ""
	defaultStorageManagerAddress = ""
	defaultRedirectDarwin        = false
	defaultCdnEnable             = false
)

const (
	// max upload part threads
	multipartCopyMaxConcurrency = 10
	// multipartCopyThresholdSize defines the default object size
	// above which multipart copy will be used. (PUT Object - Copy is used
	// for objects at or below this size.)  Empirically, 32 MB is optimal.
	multipartCopyThresholdSize = 1024 * 5 << 20 // 128MB
	// multipartCopyChunkSize defines the default chunk size for all
	// but the last Upload Part - Copy operation of a multipart copy.
	multipartCopyChunkSize = 128 << 20
)

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by tencentyun cos
type Driver struct {
	baseEmbed
}

type driver struct {
	Client                *cos.Client
	AccelerateClient      *cos.Client
	SecretID              string
	SecretKey             string
	RootDirectory         string
	ChunkSize             int64
	StorageManagerAddress string
	RedirectDarwin        bool
	CdnEnable             bool
	CdnDomain             string
	CdnKey                string
	CdnExcludeIPNets      []*net.IPNet
	CciAddress            string
	CheckCvmIp            bool
	// 是否开启了 COS 的全球加速
	AccelerateEnabled bool

	// grpcConnPool is a gRPC client connection pool.
	grpcConnPool *grpc.ConnPool
}

// DriverParameters A struct that encapsulates all of the driver parameters after all values have been set
type DriverParameters struct {
	SecretID              string
	SecretKey             string
	Bucket                string
	AppId                 string
	Region                string
	CosDomain             string
	Secure                bool
	ChunkSize             int64
	RootDirectory         string
	StorageManagerAddress string
	RedirectDarwin        bool
	CdnEnable             bool
	CdnDomain             string
	CdnKey                string
	CdnExcludeCIDRs       string
	CciAddress            string
	CheckCvmIp            bool
	// 是否开启了 COS 的全球加速
	AccelerateEnabled bool
}

func init() {
	factory.Register(driverName, &cosDriverFactory{})
}

type cosDriverFactory struct{}

func (factory *cosDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

func (d *driver) Name() string {
	return driverName
}

// FromParameters constructs a new Driver with a given parameters map
// Required parameters:
// - SecretID
// - SecretKey
// - Bucket
// - Region
func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	secretID, ok := parameters["secretid"]
	if !ok {
		return nil, fmt.Errorf("No secretid parameter provided")
	}
	secretKey, ok := parameters["secretkey"]
	if !ok {
		return nil, fmt.Errorf("No secretkey parameter provided")
	}
	regionName, ok := parameters["region"]
	if !ok || fmt.Sprint(regionName) == "" {
		return nil, fmt.Errorf("No region parameter provided")
	}
	bucket, ok := parameters["bucket"]
	if !ok || fmt.Sprint(bucket) == "" {
		return nil, fmt.Errorf("No bucket parameter provided")
	}
	appid, ok := parameters["appid"]
	if !ok || fmt.Sprint(appid) == "" {
		return nil, fmt.Errorf("No appid parameter provided")
	}
	cosDomain, ok := parameters["domain"]
	if !ok || fmt.Sprint(cosDomain) == "" {
		// 未配置 env，则置空
		cosDomain = ""
	}

	rootDir, ok := parameters["rootdir"]
	if !ok {
		rootDir = defaultRootDirectory
	}

	// storage-manager gRPC server addr
	smAddress, ok := parameters["smaddress"]
	if !ok {
		smAddress = defaultStorageManagerAddress
	}

	secureBool := true
	secure := parameters["secure"]
	switch secure := secure.(type) {
	case string:
		b, err := strconv.ParseBool(secure)
		if err != nil {
			return nil, fmt.Errorf("the secure parameter should be a boolean")
		}
		secureBool = b
	case bool:
		secureBool = secure
	case nil:
		// do nothing
	default:
		return nil, fmt.Errorf("the secure parameter should be a boolean")
	}

	chunkSize := int64(defaultChunkSize)
	chunkSizeParam, ok := parameters["chunksize"]
	if ok {
		switch v := chunkSizeParam.(type) {
		case string:
			vv, err := strconv.ParseInt(v, 0, 64)
			if err != nil {
				return nil, fmt.Errorf("chunksize parameter must be an integer, %v invalid", chunkSizeParam)
			}
			chunkSize = vv
		case int64:
			chunkSize = v
		case int, uint, int32, uint32, uint64:
			chunkSize = reflect.ValueOf(v).Convert(reflect.TypeOf(chunkSize)).Int()
		default:
			return nil, fmt.Errorf("invalid valud for chunksize: %#v", chunkSizeParam)
		}

		if chunkSize < minChunkSize {
			return nil, fmt.Errorf("The chunksize %#v parameter should be a number that is larger than or equal to %d", chunkSize, minChunkSize)
		}
	}
	redirectDarwinBool := false
	redirectDarwin, ok := parameters["redirectdarwin"]
	if ok {
		redirectDarwinBool, ok = redirectDarwin.(bool)
		if !ok {
			return nil, fmt.Errorf("The redirectdarwin parameter should be a boolean")
		}
	} else {
		redirectDarwinBool = defaultRedirectDarwin
	}

	cdnEnableBool := false
	cdnEnable, ok := parameters["cdnenable"]
	if ok {
		cdnEnableBool, ok = cdnEnable.(bool)
		if !ok {
			return nil, fmt.Errorf("The cdnenable parameter should be a boolean")
		}
	} else {
		cdnEnableBool = defaultCdnEnable
	}
	var cdnDomain interface{}
	var cdnKey interface{}
	var cdnExcludeCIDRs interface{} = ""
	var cciAddress interface{}
	var checkCvmIp interface{}
	if cdnEnableBool {
		cdnDomain, ok = parameters["cdndomain"]
		if !ok {
			return nil, fmt.Errorf("No cdndomain parameter provided while cdnenable parameter is true")
		}
		cdnKey, ok = parameters["cdnkey"]
		if !ok {
			return nil, fmt.Errorf("No cdnkey parameter provided while cdnenable parameter is true")
		}
		cdnExcludeCIDRs, ok = parameters["cdnexcludecidrs"]
		if !ok {
			cdnExcludeCIDRs = ""
		}
		cciAddress, ok = parameters["cciaddress"]
		if !ok {
			cciAddress = ""
		}

		checkCvmIp = parameters["checkcvmip"]
	}

	var parsedCheckCvmIp bool
	if checkCvmIp != nil {
		parsedCheckCvmIp = checkCvmIp.(bool)
	}

	// 加速域名支持的接口:
	// PutObject
	// PostObject
	// GetObject
	// HeadObject
	// OptionsObject
	// InitiateMultipartUpload
	// ListParts
	// UploadPart
	// AbortMultipartUpload
	// CompleteMultipartUpload
	// ListMultipartUploads
	var parsedAccelerateEnabled bool
	accelerateEnabled, ok := parameters["accelerateenabled"]
	if ok {
		parsedAccelerateEnabled, ok = accelerateEnabled.(bool)
		if !ok {
			parsedAccelerateEnabled = false
		}
	}

	params := DriverParameters{
		SecretID:              fmt.Sprint(secretID),
		SecretKey:             fmt.Sprint(secretKey),
		Bucket:                fmt.Sprint(bucket),
		AppId:                 fmt.Sprint(appid),
		Region:                fmt.Sprint(regionName),
		CosDomain:             fmt.Sprint(cosDomain),
		ChunkSize:             chunkSize,
		Secure:                secureBool,
		RootDirectory:         fmt.Sprint(rootDir),
		StorageManagerAddress: fmt.Sprint(smAddress),
		RedirectDarwin:        redirectDarwinBool,
		CdnEnable:             cdnEnableBool,
		CdnDomain:             fmt.Sprint(cdnDomain),
		CdnKey:                fmt.Sprint(cdnKey),
		CdnExcludeCIDRs:       fmt.Sprint(cdnExcludeCIDRs),
		CciAddress:            fmt.Sprint(cciAddress),
		CheckCvmIp:            parsedCheckCvmIp,
		AccelerateEnabled:     parsedAccelerateEnabled,
	}

	logrus.Debugf("COS Driver Parameters: %+v", params)

	return New(params)
}

func NewBucketURL(bucketName, region, domain string, secure bool) *url.URL {
	schema := "https"
	if !secure {
		schema = "http"
	}

	bucketURLTemplate := template.Must(
		template.New("bucketURLFormat").Parse(
			"{{.Schema}}://{{.BucketName}}.cos.{{.Region}}.{{.Domain}}",
		),
	)

	// 默认值
	if domain == "" {
		domain = "myqcloud.com"
	}

	w := bytes.NewBuffer(nil)
	bucketURLTemplate.Execute(w, struct {
		Schema     string
		BucketName string
		Region     string
		Domain     string
	}{
		schema, bucketName, region, domain,
	})

	u, _ := url.Parse(w.String())
	return u
}

// New constructs a new Driver with the given params
func New(params DriverParameters) (*Driver, error) {
	b := &cos.BaseURL{BucketURL: NewBucketURL(params.Bucket+"-"+params.AppId, params.Region, params.CosDomain, params.Secure)}
	client := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			// 填写用户账号密钥信息，也可以设置为环境变量
			SecretID:  params.SecretID,
			SecretKey: params.SecretKey,
		},
	})

	// Accelerate Client
	var accelerateClient *cos.Client
	if params.AccelerateEnabled {
		accelerateBucketUrl := &cos.BaseURL{
			BucketURL: NewBucketURL(params.Bucket+"-"+params.AppId, "accelerate", params.CosDomain, params.Secure),
		}

		accelerateClient = cos.NewClient(accelerateBucketUrl, &http.Client{
			Transport: &cos.AuthorizationTransport{
				SecretID:  params.SecretID,
				SecretKey: params.SecretKey,
			},
		})
	}

	var cdnExcludeIPNets []*net.IPNet
	if params.CdnExcludeCIDRs != "" {
		for _, cidr := range strings.Split(params.CdnExcludeCIDRs, ",") {
			cidr = strings.TrimSpace(cidr)
			if cidr == "" {
				continue
			}
			_, ipNet, err := net.ParseCIDR(cidr)
			if err != nil {
				return nil, fmt.Errorf("fail to parse CIDR '%s': %v", cidr, err)
			}
			cdnExcludeIPNets = append(cdnExcludeIPNets, ipNet)
		}
	}
	d := &driver{
		Client:                client,
		AccelerateClient:      accelerateClient,
		SecretID:              params.SecretID,
		SecretKey:             params.SecretKey,
		RootDirectory:         params.RootDirectory,
		ChunkSize:             params.ChunkSize,
		StorageManagerAddress: params.StorageManagerAddress,
		RedirectDarwin:        params.RedirectDarwin,
		CdnEnable:             params.CdnEnable,
		CdnDomain:             params.CdnDomain,
		CdnKey:                params.CdnKey,
		CdnExcludeIPNets:      cdnExcludeIPNets,
		CciAddress:            params.CciAddress,
		CheckCvmIp:            params.CheckCvmIp,
		AccelerateEnabled:     params.AccelerateEnabled,
		grpcConnPool:          grpc.NewConnPool(),
	}
	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: base.NewRegulator(d, 100),
			},
		},
	}, nil
}

func (d *driver) getContentType() string {
	return "application/octet-stream"
}

func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {

	cosPath, err := d.cosPath(path, ctx)

	if err != nil {
		return nil, err
	}

	resp, err := d.getClient().Object.Get(ctx, cosPath, nil)
	if err != nil {
		return nil, parseError(cosPath, err)
	}
	bs, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	return bs, nil
}

func (d *driver) PutContent(ctx context.Context, path string, content []byte) error {
	body := bytes.NewBuffer(content)
	opt := &cos.ObjectPutOptions{
		ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
			ContentType:   d.getContentType(),
			ContentLength: len(content),
		},
	}

	cosPath, err := d.cosPath(path, ctx)

	if err != nil {
		return err
	}

	_, err = d.Client.Object.Put(ctx, cosPath, body, opt)
	if err != nil {
		return parseError(cosPath, err)
	}
	return nil
}

// Reader retrieves an io.ReadCloser for the content stored at "path"
// with a given byte offset.
// May be used to resume reading a stream by providing a nonzero offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	opt := &cos.ObjectGetOptions{
		Range: "bytes=" + strconv.FormatInt(offset, 10) + "-",
	}

	cosPath, err := d.cosPath(path, ctx)

	if err != nil {
		return nil, err
	}

	resp, err := d.getClient().Object.Get(ctx, cosPath, opt)
	if err != nil {
		return nil, parseError(cosPath, err)
	}
	return resp.Body, nil
}

func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {

	key, err := d.cosPath(path, ctx)

	if err != nil {
		return nil, err
	}

	if !append {
		multi, _, err := d.Client.Object.InitiateMultipartUpload(ctx, key, nil)
		if err != nil {
			return nil, parseError(key, err)
		}
		uploadID := multi.UploadID
		return d.newWriter(key, uploadID, nil), nil
	}

	// list parts on uploading
	uploadIDMarker := ""
	isTruncated := true
	for isTruncated {
		v, _, err := d.Client.Bucket.ListMultipartUploads(ctx, &cos.ListMultipartUploadsOptions{
			Prefix:         key,
			UploadIDMarker: uploadIDMarker,
		})
		if err != nil {
			return nil, parseError(key, err)
		}
		uploadIDMarker = v.NextUploadIDMarker
		isTruncated = v.IsTruncated

		for _, upload := range v.Uploads {
			if key != upload.Key {
				continue
			}
			uploadID := upload.UploadID

			parts, err := d.listParts(ctx, key, uploadID)
			if err != nil {
				return nil, parseError(key, err)
			}

			return d.newWriter(key, uploadID, parts), nil
		}
	}

	return nil, storagedriver.PathNotFoundError{Path: key}
}

func (d *driver) List(ctx context.Context, opath string) ([]string, error) {
	subPath := opath
	if subPath != "/" && opath[len(subPath)-1] != '/' {
		subPath = subPath + "/"
	}

	rootCOSPath, err := d.cosPath("", ctx)

	if err != nil {
		return nil, err
	}

	// This is to cover for the cases when the rootDirectory of the driver is either "" or "/".
	// In those cases, there is no root prefix to replace and we must actually add a "/" to all
	// results in order to keep them as valid paths as recognized by storagedriver.PathRegexp
	prefix := ""
	if rootCOSPath == "" {
		prefix = "/"
	}

	cosPath, err := d.cosPath(subPath, ctx)

	if err != nil {
		return nil, err
	}

	listResponse, _, err := d.Client.Bucket.Get(ctx, &cos.BucketGetOptions{
		Prefix:    cosPath,
		Delimiter: "/",
		MaxKeys:   listMax,
	})
	if err != nil {
		return nil, parseError(cosPath, err)
	}

	files := []string{}
	directories := []string{}

	for {
		for _, key := range listResponse.Contents {

			f := path.Base(key.Key)
			files = append(files, path.Join(opath, f))
		}

		for _, commonPrefix := range listResponse.CommonPrefixes {
			directories = append(directories, path.Join(subPath, path.Base(commonPrefix)))
		}

		if listResponse.IsTruncated {
			listResponse, _, err = d.Client.Bucket.Get(ctx, &cos.BucketGetOptions{
				Prefix:    cosPath,
				Delimiter: "/",
				MaxKeys:   listMax,
				Marker:    listResponse.NextMarker,
			})
			if err != nil {
				return nil, err
			}
		} else {
			break
		}
	}

	// This is to cover for the cases when the first key equal to cosPath.
	if len(files) > 0 && files[0] == strings.Replace(cosPath, rootCOSPath, prefix, 1) {
		files = files[1:]
	}

	if opath != "/" {
		if len(files) == 0 && len(directories) == 0 {
			// Treat empty response as missing directory, since we don't actually
			// have directories in s3.
			return nil, storagedriver.PathNotFoundError{Path: cosPath}
		}
	}

	return append(files, directories...), nil
}

func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {

	cosPath, err := d.cosPath(path, ctx)

	if err != nil {
		return nil, err
	}

	opt := &cos.BucketGetOptions{
		Prefix:  cosPath,
		MaxKeys: 1,
	}
	listResponse, _, err := d.Client.Bucket.Get(ctx, opt)
	if err != nil {
		return nil, err
	}

	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	if len(listResponse.Contents) == 1 {
		if listResponse.Contents[0].Key != cosPath {
			fi.IsDir = true
		} else {
			fi.IsDir = false
			fi.Size = int64(listResponse.Contents[0].Size)

			timestamp, err := time.Parse(time.RFC3339Nano, listResponse.Contents[0].LastModified)
			if err != nil {
				return nil, err
			}
			fi.ModTime = timestamp
		}
	} else if len(listResponse.CommonPrefixes) == 1 {
		fi.IsDir = true
	} else {
		return nil, storagedriver.PathNotFoundError{Path: cosPath}
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	// need to implement multi-part upload
	err := d.copy(ctx, sourcePath, destPath)
	if err != nil {
		return parseError(sourcePath, err)
	}

	cosPath, err := d.cosPath(sourcePath, ctx)

	if err != nil {
		return err
	}

	_, err = d.Client.Object.Delete(ctx, cosPath)
	if err != nil {
		return parseError(sourcePath, err)
	}
	return nil
}

func (d *driver) Delete(ctx context.Context, path string) error {
	cosPath, err := d.cosPath(path, ctx)

	if err != nil {
		return err
	}

	opt := &cos.BucketGetOptions{
		Prefix:  cosPath,
		MaxKeys: listMax,
	}
	// list max objects
	listResponse, _, err := d.Client.Bucket.Get(ctx, opt)
	if err != nil || len(listResponse.Contents) == 0 {
		return storagedriver.PathNotFoundError{Path: cosPath}
	}

	cosObjects := make([]cos.Object, listMax)

	for len(listResponse.Contents) > 0 {
		numCosObjects := len(listResponse.Contents)
		for index, key := range listResponse.Contents {
			// Stop if we encounter a key that is not a subpath (so that deleting "/a" does not delete "/ab").
			if len(key.Key) > len(cosPath) && (key.Key)[len(cosPath)] != '/' {
				numCosObjects = index
				break
			}
			cosObjects[index].Key = key.Key
		}

		// delete by keys
		opt := &cos.ObjectDeleteMultiOptions{
			Objects: cosObjects[0:numCosObjects],
			Quiet:   false,
		}
		_, _, err := d.Client.Object.DeleteMulti(ctx, opt)
		if err != nil {
			// delete fail
			return parseError(path, err)
		}

		// contents contain keys which not in a subpath
		if numCosObjects < len(listResponse.Contents) {
			return nil
		}

		// fetch objects again
		listResponse, _, err = d.Client.Bucket.Get(ctx, &cos.BucketGetOptions{
			Prefix:    cosPath,
			Delimiter: "",
			Marker:    "",
			MaxKeys:   listMax,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {

	// FIXME: docker-for-mac will replace "%3d" to "=" , "/sign=a%3Db"  -> "/sign=a=b" , so sad :!
	isMac := strings.Index(dcontext.GetStringValue(ctx, "http.request.useragent"), "darwin") != -1
	if isMac && !d.RedirectDarwin {
		return "", storagedriver.ErrUnsupportedMethod{}
	}

	methodString := "GET"
	method, ok := options["method"]
	if ok {
		methodString, ok = method.(string)
		// FIXME: https://github.com/docker/distribution/issues/2649
		// ignore HEAD method
		if !ok || (methodString != "GET") {
			return "", storagedriver.ErrUnsupportedMethod{}
		}
	}
	now := time.Now()
	expiresTime := now.Add(20 * time.Minute)
	expires, ok := options["expiry"]
	if ok {
		et, ok := expires.(time.Time)
		if ok {
			expiresTime = et
		}
	}
	duration := expiresTime.Sub(now)

	cosPath, err := d.cosPath(path, ctx)

	if err != nil {
		return "", err
	}

	logrus.Debugf("URLFor - cosPath: [%s]", cosPath)

	if d.CdnEnable && d.shouldUseCdn(ctx) {
		// encode path except slash
		cosPath = pathEscape(cosPath)
		u, err := cdn.PreSignedCDNURLTypeA(cdn.CDNConfig{
			Domain: d.CdnDomain,
			Key:    d.CdnKey,
		}, cosPath)
		if err != nil {
			return "", err
		}
		signedURL := u.String()
		stdlog.Printf("CDN Pre-Signed URL: %q\n", signedURL)
		return signedURL, nil
	} else {
		u, err := d.Client.Object.GetPresignedURL(ctx, methodString, cosPath, d.SecretID, d.SecretKey, duration, nil)
		if err != nil {
			return "", err
		}
		signedURL := u.String()
		stdlog.Printf("Origin Pre-Signed URL: %q\n", signedURL)
		return signedURL, nil
	}
}

func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {
	return storagedriver.WalkFallback(ctx, d, path, f)
}

func (d *driver) newWriter(key, uploadID string, parts []cos.Object) storagedriver.FileWriter {
	var size int64
	for _, part := range parts {
		size += int64(part.Size)
	}
	return &writer{
		driver:   d,
		key:      key,
		uploadID: uploadID,
		parts:    parts,
		size:     size,
	}
}

type writer struct {
	driver        *driver
	key           string
	uploadID      string
	parts         []cos.Object
	size          int64
	readyPart     []byte
	lastReadyPart []byte
	// pendingPart     []byte
	closed    bool
	committed bool
	cancelled bool
}

func (w *writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("already closed")
	} else if w.committed {
		return 0, fmt.Errorf("already committed")
	} else if w.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}

	content := p

	chunkNumber := len(w.parts) + 1

	chunkSize := len(content)

	// 不为空 且 满足大小，开始上传
	if w.readyPart != nil && len(w.readyPart) > minChunkSize {
		content = append(w.readyPart, p...)

		resp, err := w.driver.Client.Object.UploadPart(
			context.Background(), w.key, w.uploadID, chunkNumber, bytes.NewReader(content), nil,
		)

		if err != nil {
			return 0, err
		}

		w.parts = append(w.parts, cos.Object{
			ETag:       resp.Header.Get("Etag"),
			PartNumber: chunkNumber,
		})
		w.size += int64(len(content))
		w.lastReadyPart = content
		w.readyPart = nil

		return chunkSize, nil
	}
	// add to ready list
	w.readyPart = append(w.readyPart, p...)

	return chunkSize, nil
}

func (w *writer) Size() int64 {
	return w.size
}

func (w *writer) Close() error {
	if w.closed {
		return fmt.Errorf("already closed")
	}
	w.closed = true
	return w.flushPart()
}

func (w *writer) Cancel() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	}
	w.cancelled = true
	_, err := w.driver.Client.Object.AbortMultipartUpload(context.Background(), w.key, w.uploadID)
	return err
}

func (w *writer) Commit() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	} else if w.cancelled {
		return fmt.Errorf("already cancelled")
	}
	err := w.flushPart()
	if err != nil {
		return err
	}
	w.committed = true

	opt := &cos.CompleteMultipartUploadOptions{
		Parts: w.parts,
	}
	_, _, err = w.driver.Client.Object.CompleteMultipartUpload(context.Background(), w.key, w.uploadID, opt)
	if err != nil {
		w.driver.Client.Object.AbortMultipartUpload(context.Background(), w.key, w.uploadID)
		return err
	}
	return nil
}

// flushPart flushes buffers to write a part to cos.
// Only called by Write (with both buffers full) and Close/Commit (always)
func (w *writer) flushPart() error {

	if len(w.readyPart) != 0 {
		// 覆盖掉最后一次上传的 chunk
		chunkNumber := len(w.parts)

		if chunkNumber == 0 {
			chunkNumber = 1
		}

		content := append(w.lastReadyPart, w.readyPart...)

		resp, err := w.driver.Client.Object.UploadPart(
			context.Background(), w.key, w.uploadID, chunkNumber, bytes.NewReader(content), nil,
		)

		if err != nil {
			return err
		}

		end := 0

		if len(w.parts) > 0 {
			end = len(w.parts) - 1
		}

		w.parts = append(w.parts[0:end], cos.Object{
			ETag:       resp.Header.Get("Etag"),
			PartNumber: chunkNumber,
		})
		w.size += int64(len(w.readyPart))
		w.lastReadyPart = content
		w.readyPart = nil

	}
	return nil
}

func isDir(s string) bool {
	return strings.HasSuffix(s, "/")
}

func toDir(s string) string {

	if s != "/" && s[len(s)-1] != '/' {
		return s + "/"
	}

	return s

}

func (d *driver) resolvePath(before, after string) string {

	after = path.Join(d.RootDirectory, after)

	if isDir(before) {
		after = toDir(after)
	}

	return strings.TrimLeft(after, "/")

}

func (d *driver) cosPath(subPath string, ctx context.Context) (string, error) {

	if d.StorageManagerAddress != "" {

		storagePath, err := manager.GetStoragePath(d.grpcConnPool,
			d.StorageManagerAddress,
			dcontext.GetStringValue(ctx, "http.request.host"),
			subPath)

		if err != nil {
			return "", fmt.Errorf("get storage path failed: %v", err)
		}

		return d.resolvePath(subPath, storagePath), nil

	}

	return d.resolvePath(subPath, path.Join(subPath)), nil

}

// copy copies an object stored at sourcePath to destPath.
func (d *driver) copy(ctx context.Context, sourcePath string, destPath string) error {
	fileInfo, err := d.Stat(ctx, sourcePath)
	if err != nil {
		return err
	}

	parsedSourcePath, err := d.cosPath(sourcePath, ctx)

	if err != nil {
		return err
	}

	parsedDestPath, err := d.cosPath(destPath, ctx)

	if err != nil {
		return err
	}

	sourceURL := fmt.Sprintf("%s/%s", d.Client.BaseURL.BucketURL.Host, parsedSourcePath)

	if fileInfo.Size() <= multipartCopyThresholdSize {
		_, _, err := d.Client.Object.Copy(ctx, parsedDestPath, sourceURL, nil)
		if err != nil {
			return err
		}
		return nil
	}

	// upload parts
	createResp, _, err := d.Client.Object.InitiateMultipartUpload(ctx, parsedDestPath, &cos.InitiateMultipartUploadOptions{
		ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
			ContentType: d.getContentType(),
		},
	})

	if err != nil {
		return err
	}

	numParts := (fileInfo.Size() + multipartCopyChunkSize - 1) / multipartCopyChunkSize
	parts := make([]cos.Object, numParts)

	errChan := make(chan error, numParts)
	limiter := make(chan struct{}, multipartCopyMaxConcurrency)

	for i := range parts {
		i := int64(i)
		go func() {
			defer func() {
				if err := recover(); err != nil {
					logrus.Errorf("copy part sourcePath: %s destPath: %s error: %v", sourcePath, destPath, err)
				}
			}()

			limiter <- struct{}{}
			firstByte := i * multipartCopyChunkSize
			lastByte := firstByte + multipartCopyChunkSize - 1
			if lastByte >= fileInfo.Size() {
				lastByte = fileInfo.Size() - 1
			}
			uploadResp, _, err := d.Client.Object.CopyPart(ctx, parsedDestPath, createResp.UploadID, int(i+1), sourceURL, &cos.ObjectCopyPartOptions{
				XCosCopySource:      fmt.Sprintf("%s/%s", d.Client.BaseURL.BucketURL.Host, parsedSourcePath),
				XCosCopySourceRange: fmt.Sprintf("bytes=%d-%d", firstByte, lastByte),
			})

			if err == nil {
				parts[i] = cos.Object{
					ETag:       uploadResp.ETag,
					PartNumber: int(i + 1),
				}
			}
			errChan <- err
			<-limiter
		}()
	}

	fullyCompleted := true
	for range parts {
		err := <-errChan
		if err != nil {
			fullyCompleted = false
		}
	}

	if fullyCompleted {
		_, _, err = d.Client.Object.CompleteMultipartUpload(ctx, parsedDestPath, createResp.UploadID, &cos.CompleteMultipartUploadOptions{
			Parts: parts,
		})
	} else {
		_, err = d.Client.Object.AbortMultipartUpload(ctx, parsedDestPath, createResp.UploadID)
	}
	return err
}

func (d *driver) shouldUseCdn(ctx context.Context) bool {
	r, ok := ctx.Value("http.request").(*http.Request)
	if !ok {
		return true
	}

	ip := net.ParseIP(getClientIP(r))
	if ip == nil {
		return true
	}
	for _, ipNet := range d.CdnExcludeIPNets {
		if ipNet.Contains(ip) {
			return false
		}
	}

	if !d.CheckCvmIp {
		return true
	}

	has, err := cci.HasCvmIp(d.grpcConnPool, d.CciAddress, ip.String())
	if err != nil {
		logrus.Warnf("failed to check if CCI has the specified CVM IP %q: %v", ip.String(), err)
		return true
	}
	return !has
}

func (d *driver) listParts(ctx context.Context, key, uploadID string) ([]cos.Object, error) {
	var parts []cos.Object

	partNumberMarker := ""
	isTruncated := true
	for isTruncated {
		v, _, err := d.Client.Object.ListParts(ctx, key, uploadID, &cos.ObjectListPartsOptions{
			PartNumberMarker: partNumberMarker,
		})
		if err != nil {
			return nil, err
		}
		partNumberMarker = v.NextPartNumberMarker
		isTruncated = v.IsTruncated

		parts = append(parts, v.Parts...)
	}

	return parts, nil
}

func (d *driver) getClient() *cos.Client {
	if d.AccelerateEnabled && d.AccelerateClient != nil {
		return d.AccelerateClient
	}
	return d.Client
}

func parseError(path string, err error) error {
	if cosErr, ok := err.(*cos.ErrorResponse); ok && cosErr.Response.StatusCode == http.StatusNotFound && (cosErr.Code == "NoSuchKey" || cosErr.Code == "") {
		return storagedriver.PathNotFoundError{Path: path}
	}

	return err
}

func getClientIP(r *http.Request) string {
	clientIP := r.Header.Get("X-Forwarded-For")
	clientIP = strings.TrimSpace(strings.Split(clientIP, ",")[0])
	if clientIP == "" {
		clientIP = strings.TrimSpace(r.Header.Get("X-Real-Ip"))
	}
	if clientIP != "" {
		return clientIP
	}

	if ip, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr)); err == nil {
		return ip
	}

	return ""
}

func pathEscape(p string) string {
	const delimiter = "/"

	paths := strings.Split(p, delimiter)

	escapedPath := make([]string, len(paths))
	for i, v := range paths {
		escapedPath[i] = url.PathEscape(v)
	}

	return strings.Join(escapedPath, delimiter)
}
