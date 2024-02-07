FROM golang:1.21.6-alpine3.19 as builder

WORKDIR /app

# Required for ssl certificate
RUN apk add ca-certificates just && update-ca-certificates

# grpcurl required for health check probes
RUN cd ../tmp && wget https://github.com/fullstorydev/grpcurl/releases/download/v1.8.6/grpcurl_1.8.6_linux_x86_64.tar.gz && tar -xvf grpcurl_1.8.6_linux_x86_64.tar.gz && chmod +x ./grpcurl

COPY go.mod go.sum ./

RUN go mod download

COPY . ./

RUN just build

# Minimized Final image
FROM alpine:3.19

RUN apk --no-cache add ca-certificates

COPY --from=builder /tmp/grpcurl /bin/grpcurl
COPY --from=builder /app/build/exec-monitor /var/run/app

# Create user and group
ENV APP_USER=celery-monitor-executor
RUN addgroup -S $APP_USER && adduser -S $APP_USER -G $APP_USER
RUN chown -R $APP_USER:$APP_USER /var/run/app
USER $APP_USER

ARG release
ENV RELEASE_SHA $release

ENTRYPOINT ["sh", "-c"]
CMD ["/var/run/app"]
