from felix.utils import run_cmd
import pytest

def test_run_cmd_success(tmp_path):
    script = tmp_path / "echo.sh"
    script.write_text("#!/bin/sh\necho hi\n")
    script.chmod(0o755)
    assert run_cmd([str(script)]) == "hi"

def test_run_cmd_fail(tmp_path):
    script = tmp_path / "fail.sh"
    script.write_text("#!/bin/sh\nexit 1\n")
    script.chmod(0o755)
    with pytest.raises(RuntimeError):
        run_cmd([str(script)])