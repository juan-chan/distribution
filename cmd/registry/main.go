package main

import (
	_ "net/http/pprof"

	"github.com/juan-chan/distribution/registry"
	_ "github.com/juan-chan/distribution/registry/auth/htpasswd"
	_ "github.com/juan-chan/distribution/registry/auth/silly"
	_ "github.com/juan-chan/distribution/registry/auth/token"
	_ "github.com/juan-chan/distribution/registry/proxy"
	_ "github.com/juan-chan/distribution/registry/storage/driver/azure"
	_ "github.com/juan-chan/distribution/registry/storage/driver/filesystem"
	_ "github.com/juan-chan/distribution/registry/storage/driver/gcs"
	_ "github.com/juan-chan/distribution/registry/storage/driver/inmemory"
	_ "github.com/juan-chan/distribution/registry/storage/driver/middleware/alicdn"
	_ "github.com/juan-chan/distribution/registry/storage/driver/middleware/cloudfront"
	_ "github.com/juan-chan/distribution/registry/storage/driver/middleware/redirect"
	_ "github.com/juan-chan/distribution/registry/storage/driver/oss"
	_ "github.com/juan-chan/distribution/registry/storage/driver/s3-aws"
	_ "github.com/juan-chan/distribution/registry/storage/driver/swift"
)

func main() {
	registry.RootCmd.Execute()
}
