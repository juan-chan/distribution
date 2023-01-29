ARG GO_VERSION=1.13.8

FROM coding-public-docker.pkg.coding.net/public/docker/golang:${GO_VERSION}-alpine3.11 AS build

ENV DISTRIBUTION_DIR /go/src/github.com/reedchan7/distribution
ENV BUILDTAGS include_oss include_cos include_gcs
ENV GO_BUILD_FLAGS -mod=vendor

ARG GOOS=linux
ARG GOARCH=amd64
ARG GOARM=6
ARG VERSION
ARG REVISION

RUN printf "https://mirrors.ustc.edu.cn/alpine/v3.11/main\nhttps://mirrors.ustc.edu.cn/alpine/v3.11/community" > /etc/apk/repositories

RUN set -ex \
    && apk add --no-cache make git file

WORKDIR $DISTRIBUTION_DIR
COPY . $DISTRIBUTION_DIR
RUN CGO_ENABLED=0 make PREFIX=/go clean binaries && file ./bin/registry | grep "statically linked"

FROM alpine:3.11

RUN printf "https://mirrors.ustc.edu.cn/alpine/v3.11/main\nhttps://mirrors.ustc.edu.cn/alpine/v3.11/community" > /etc/apk/repositories

RUN set -ex \
    && apk add --no-cache ca-certificates apache2-utils

COPY cmd/registry/config-dev.yml /etc/docker/registry/config.yml
COPY start-server.sh /etc/docker/registry/start-server.sh
COPY --from=build /go/src/github.com/reedchan7/distribution/bin/registry /bin/registry
COPY --from=build /usr/local/go/lib/time/zoneinfo.zip /opt

RUN mkdir /usr/share/zoneinfo \
    && unzip -q /opt/zoneinfo.zip -d /usr/share/zoneinfo \
    && rm /opt/zoneinfo.zip \
    && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

VOLUME ["/var/lib/registry"]
EXPOSE 5000
CMD ["/etc/docker/registry/start-server.sh"]
