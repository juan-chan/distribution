package authserver

import (
	"context"
	"time"

	pb "github.com/juan-chan/distribution/registry/auth/authserver/proto"
	"github.com/juan-chan/distribution/registry/grpc"
)

func CheckTag(connPool *grpc.ConnPool, address, host, repo, tag string) (*pb.DockerCheckTagResponse, error) {
	conn, err := connPool.Get(address)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancel()

	return pb.NewArtAuthClient(conn).DockerCheckTag(ctx, &pb.DockerCheckTagRequest{
		Host: host,
		Repo: repo,
		Tag:  tag,
	})
}

// IsImageForbidden 检验所指定的 Docker Image Tag 是否已被禁止拉取
func IsImageForbidden(connPool *grpc.ConnPool, addr, host, repo, tag string) (*pb.IsDockerImageForbiddenResponse, error) {
	conn, err := connPool.Get(addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancel()

	return pb.NewArtAuthClient(conn).IsDockerImageForbidden(ctx, &pb.IsDockerImageForbiddenRequest{
		Host: host,
		Repo: repo,
		Tag:  tag,
	})
}
