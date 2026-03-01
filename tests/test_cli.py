# tests/test_cli.py
import tempfile
import os
import pytest
import yaml
from click.testing import CliRunner
from src.cli import cli


@pytest.fixture
def config_file():
    cfg = {
        "mysql": {"host": "localhost", "databases": []},
        "bigquery": {"projects": []},
        "output": {"database_path": ""},
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(cfg, f)
        path = f.name
    yield path
    os.unlink(path)


def test_cli_status_runs(config_file):
    """The status command runs without error."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # Update config to use temp db
    with open(config_file) as f:
        cfg = yaml.safe_load(f)
    cfg["output"]["database_path"] = db_path
    with open(config_file, "w") as f:
        yaml.dump(cfg, f)

    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--config", config_file])
    assert result.exit_code == 0
    os.unlink(db_path)


def test_cli_help():
    """The CLI shows help text."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "analyze" in result.output or "discover" in result.output


def test_cli_discover_no_sources(config_file):
    """Discover with no databases/datasets configured completes cleanly."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    with open(config_file) as f:
        cfg = yaml.safe_load(f)
    cfg["output"]["database_path"] = db_path
    with open(config_file, "w") as f:
        yaml.dump(cfg, f)

    runner = CliRunner()
    result = runner.invoke(cli, ["discover", "--config", config_file])
    assert result.exit_code == 0
    os.unlink(db_path)
