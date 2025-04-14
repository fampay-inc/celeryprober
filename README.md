# Celery Monitor

An application to monitor Celery task queues and detect potentially dropped or stale tasks using a probe-based architecture.

## Features

*   **Probe-based Monitoring:** Configure multiple probes, each monitoring specific Redis instances and Celery task channels.
*   **Stale Task Detection:** Identifies tasks that haven't updated their status within a configurable timeframe.
*   **Prometheus Metrics:** Exposes metrics for task counts, event types, and stale tasks, tagged by probe name.
*   **Modes:**
    *   `server`: Runs an HTTP server exposing metrics and potentially a UI (if implemented).
    *   `cron`: Runs periodically to check for stale tasks and send reports to Slack.
*   **Configuration:** Load settings from a JSON file and/or environment variables.
*   **Slack Notifications:** Sends detailed reports of stale tasks to a configured Slack channel when running in `cron` mode.

## Configuration

The application can be configured using a JSON file and environment variables. Environment variables take precedence over the JSON file.

1.  **JSON Configuration File:**
    *   A sample configuration file is provided at `config.sample.json`.
    *   Specify the path to your configuration file using the `CONFIG_FILE` environment variable.
    *   See `src/config.go` for the structure and available options (e.g., global settings, probe definitions).

2.  **Environment Variables:**
    *   `APPLICATION_MODE`: Set the run mode (`server` or `cron`). Defaults to `server`.
    *   `CONFIG_FILE`: Path to the JSON configuration file.
    *   `LOG_LEVEL`: Set the logging level (e.g., `debug`, `info`, `warn`, `error`). Defaults to `info`.
    *   **For Cron Mode:**
        *   `SLACK_ACCESS_TOKEN`: Your Slack Bot User OAuth Token.
        *   `SLACK_CHANNEL_ID`: The ID of the Slack channel to send reports to.

    *   See `.env.sample` for a list of common environment variables. **Note:** `.env.sample` does not include Slack variables by default; these must be set directly in your environment for `cron` mode to function.

## Running the Application

### Using `just` (Recommended)

*   **Run in Server Mode:** `just run` (This typically runs the server mode based on the default Justfile recipe).
*   **Run in Cron Mode:** `just cron` (Assuming a `cron` recipe exists in the Justfile).
*   **Build:** `just build`
    *   Binary can be found in the `build/` directory.

### Using `go run`

Make sure you are in the project root directory.

*   **Server Mode (using sample config):**
    ```bash
    CONFIG_FILE=config.sample.json go run ./src
    ```
    (Or set `APPLICATION_MODE=server`)

*   **Cron Mode (using sample config, requires Slack env vars):**
    ```bash
    export SLACK_ACCESS_TOKEN="your-slack-token"
    export SLACK_CHANNEL_ID="your-channel-id"
    CONFIG_FILE=config.sample.json APPLICATION_MODE=cron go run ./src
    ```

## Development

*(Add any specific development setup instructions here if needed)*

## Testing

A utility is provided to send sample Celery events to Redis for testing the monitor.

*   **Send Test Events:**
    ```bash
    go run ./cmd/test_events --redis-url="redis://localhost:6379/0" --task-name="your_task_name" --num-events=10 --delay-ms=500
    ```
    *   Adjust the flags as needed:
        *   `--redis-url`: The Redis instance the monitor is watching.
        *   `--task-name`: The name of the task to simulate.
        *   `--num-events`: Number of event sequences (sent, received, started, succeeded/failed) to send.
        *   `--delay-ms`: Delay between sending events in milliseconds.
        *   *(Add descriptions for any other relevant flags here)*

## Contributing

*(Add contribution guidelines here if needed)*
