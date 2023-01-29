package main

import (
	_ "net/http/pprof"

	"github.com/reedchan7/distribution/registry"
	_ "github.com/reedchan7/distribution/registry/auth/htpasswd"
	_ "github.com/reedchan7/distribution/registry/auth/silly"
	_ "github.com/reedchan7/distribution/registry/auth/token"
	_ "github.com/reedchan7/distribution/registry/proxy"
	_ "github.com/reedchan7/distribution/registry/storage/driver/azure"
	_ "github.com/reedchan7/distribution/registry/storage/driver/filesystem"
	_ "github.com/reedchan7/distribution/registry/storage/driver/gcs"
	_ "github.com/reedchan7/distribution/registry/storage/driver/inmemory"
	_ "github.com/reedchan7/distribution/registry/storage/driver/middleware/alicdn"
	_ "github.com/reedchan7/distribution/registry/storage/driver/middleware/cloudfront"
	_ "github.com/reedchan7/distribution/registry/storage/driver/middleware/redirect"
	_ "github.com/reedchan7/distribution/registry/storage/driver/oss"
	_ "github.com/reedchan7/distribution/registry/storage/driver/s3-aws"
	_ "github.com/reedchan7/distribution/registry/storage/driver/swift"
)

func main() {
	registry.RootCmd.Execute()
}
