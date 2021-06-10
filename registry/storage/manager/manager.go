package manager

import (
	"context"
	"time"

	"github.com/juan-chan/distribution/registry/grpc"
	pb "github.com/juan-chan/distribution/registry/storage/manager/storage-path"
)

func GetStoragePath(connPool *grpc.ConnPool, address, host, subPath string) (string, error) {
	conn, err := connPool.Get(address)
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancel()

	r, err := pb.NewStoragePathClient(conn).GetDockerStoragePath(ctx, &pb.DockerStoragePathRequest{Host: host, SubPath: subPath})
	if err != nil {
		return "", err
	}

	return r.Path, nil
}
