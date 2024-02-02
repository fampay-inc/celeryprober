run flags='':
    go run {{flags}} ./src
run-cron flags='':
    APPLICATION_MODE=cron go run {{flags}} ./src
build:
    mkdir -p build
    go build -o build/exec-monitor ./src
