# tora-rs

A poor clone of [tora-hs](https://github.com/dgvncsz0f/tora/) written in Rust as an exercise.

## Usage

```bash
$ cargo run -- -q "query" -i "index-name" [-c path/to/config]
```

## Configuration

The default configuration file is expected at `$HOME/.torars_rc` and should contain the following:

```json
{
    "creds": {
        "user": "USERNAME",
        "password": "hunter2"
    }
}
```
