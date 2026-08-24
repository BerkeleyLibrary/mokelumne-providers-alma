# mokelumne-providers-alma

Airflow provider for Alma connections.

A component of Mokelumne, UC Berkeley Library's Airflow installation, libraries, and Dags.

## Dependencies

Dependencies are declared in `pyproject.toml` and pinned in `requirements.txt` with hashes for supply-chain security.

When adding or changing dependencies, regenerate the pins with [uv](https://docs.astral.sh/uv/):

```sh
uv pip compile pyproject.toml --extra test -c constraints.txt \
  --generate-hashes -o requirements.txt
```

`constraints.txt` contains upper bounds derived from the base Airflow Docker image to prevent version conflicts with pre-installed packages. Regenerate it when bumping `AIRFLOW_VERSION`:

```sh
docker run --rm --entrypoint python apache/airflow:<version> -m pip freeze
```
