package cci

import (
	"context"
	"fmt"

	"github.com/docker/distribution/registry/grpc"
	pb "github.com/docker/distribution/registry/storage/driver/cos/cci/proto"
)

func HasCvmIp(connPool *grpc.ConnPool, address, ip string) (bool, error) {
	conn, err := connPool.Get(address)
	if err != nil {
		return false, fmt.Errorf("get gRPC client connection from the pool failed: %v", err)
	}

	client := pb.NewCCIInfoServiceClient(conn)

	resp, err := client.HasCvmIp(context.Background(), &pb.CCIHasCvmIpRequest{Ip: ip})
	if err != nil {
		return false, fmt.Errorf("fail to request CCIInfoService.HasCvmIp method: %v", err)
	}

	return resp.Has, nil
}
