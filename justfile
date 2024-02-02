run flags='':
    go run {{flags}} ./src
build:
    mkdir -p build
    go build -o build/exec-monitor ./src
