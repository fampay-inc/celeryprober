run flags='':
    go run {{flags}} ./src
run-cron flags='':
    APPLICATION_MODE=cron go run {{flags}} ./src
build flags='':
    mkdir -p build
    CGO_ENABLED=0 go build -ldflags="-w -s" {{flags}} -o build/exec-monitor ./src
