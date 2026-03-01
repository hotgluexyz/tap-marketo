# tap-marketo

`tap-marketo` is a Singer tap for [marketo](https://www.marketo.com/), a workforce and labor management platform for restaurants.

Built with the [Hotglue Singer SDK](https://github.com/hotgluexyz/HotglueSingerSDK) for Singer Taps.

## Installation

```bash
pip install tap-marketo
```

Or install directly from the repository:

```bash
pip install git+https://github.com/hotgluexyz/tap-marketo.git
```

## Configuration

### Accepted Config Options

| Setting         | Required | Description                                                                 |
|-----------------|----------|-----------------------------------------------------------------------------|
| `client_id`     | Yes      | marketo OAuth2 client ID                                                    |
| `client_secret` | Yes      | marketo OAuth2 client secret                                                |
| `start_date`    | No       | The earliest record date to sync (ISO 8601 / datetime format)               |

Example `config.json`:

```json
{
  "client_id": "your_client_id",
  "client_secret": "your_client_secret",
  "start_date": "2024-01-01T00:00:00Z"
}
```

A full list of supported settings and capabilities for this tap is available by running:

```bash
tap-marketo --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

This tap uses OAuth2 client credentials authentication. You can obtain API credentials (client ID, client secret, and company GUID) from marketo via Company Settings → Developer Tools, or use OAuth 2.0 for partner integrations.


You can easily run `tap-marketo` by itself or in a pipeline.

### Executing the Tap Directly

```bash
tap-marketo --version
tap-marketo --help
tap-marketo --config CONFIG --discover > ./catalog.json
tap-marketo --config CONFIG --catalog CATALOG > ./data.singer
```

## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_marketo/tests` subfolder and then run:

```bash
poetry run pytest
```

You can also test the `tap-marketo` CLI interface directly using `poetry run`:

```bash
poetry run tap-marketo --help
```
