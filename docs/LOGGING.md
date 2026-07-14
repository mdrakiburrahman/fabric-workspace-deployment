# Logging

Every invocation reads a **mandatory** `common.local.logging` block from the deployment
configuration file. It controls **where** logs are written and **how** they rotate, replacing
the previous behavior of dumping a single unbounded `app_<timestamp>.log` into the process
working directory.

## Configuration

```jsonc
{
  "common": {
    "local": {
      "rootFolder": "{git-root}",
      "logging": {
        "rootFolder": "{git-root}/logs",
        "maxSizeMB": 10,
        "backupCount": 20,
        "level": "INFO"
      }
    }
  }
}
```

| Key           | Type    | Required | Description                                                                   |
| ------------- | ------- | -------- | ----------------------------------------------------------------------------- |
| `rootFolder`  | string  | yes      | Base directory for logs. Supports the `{git-root}` placeholder. Auto-created. |
| `maxSizeMB`   | number  | yes      | Rollover threshold per file, in MB (must be `> 0`).                           |
| `backupCount` | integer | yes      | Number of rotated files to keep; the oldest is deleted (must be `>= 1`).      |
| `level`       | string  | yes      | Console log level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`.        |

The `logging` block sits next to the existing deployment-source `rootFolder`, but the two are
independent: the deployment `rootFolder` must already exist and contain the source to deploy,
whereas the logging `rootFolder` is **created automatically**.

## Per-invocation layout

Each run writes into its own timestamped subfolder under `rootFolder`, using a native
[`RotatingFileHandler`](https://docs.python.org/3/library/logging.handlers.html#logging.handlers.RotatingFileHandler).
Within that subfolder, `app.log` is the active file; once it reaches `maxSizeMB` it rolls over
to `app.log.1`, `app.log.2`, and so on, keeping the newest `backupCount` segments:

```
<rootFolder>/
  app_20260714_135201/     # one subfolder per invocation (run start timestamp)
    app.log                # active log
    app.log.1              # rotated segments (newest first)
    app.log.2
    ...
    app.log.<backupCount>  # oldest kept; beyond this the oldest is deleted
```

## Levels

The file handler always captures `DEBUG` for full detail, while the console honors the
configured `level`. Console output additionally filters out noisy `fabric_cicd` logger records.

## Placeholders

The `rootFolder` value supports the `{git-root}` placeholder, resolved from the `GIT_ROOT`
environment variable or `git rev-parse --show-toplevel`. The per-invocation subfolder is already
timestamped, so no `{timestamp}` placeholder is needed.

## Fail-fast validation

The logging configuration is parsed and validated **before** any deployment work begins. A run
fails fast with a clear error if:

- the `common.local.logging` block or any of its keys is missing,
- `rootFolder` is empty,
- `maxSizeMB` is not a number greater than `0`,
- `backupCount` is not an integer greater than or equal to `1`, or
- `level` is not one of `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`.
