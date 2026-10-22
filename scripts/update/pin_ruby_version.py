import os
import re
import sys
import tempfile
from pathlib import Path

version = os.environ.get("RUBY_VERSION_PIN", "")
if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version) is None:
    raise SystemExit("RUBY_VERSION_PIN must be a stable X.Y.Z release")

if len(sys.argv) != 2:
    raise SystemExit("usage: pin_ruby_version.py <.ruby-version path>")

path = Path(sys.argv[1])
try:
    current = path.read_text(encoding="utf-8")
except OSError as error:
    raise SystemExit(f"cannot read {path}: {error}") from error

if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+\n?", current) is None:
    raise SystemExit(f"{path} must contain exactly one stable X.Y.Z version")

temporary_path = None
try:
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    ) as temporary:
        temporary.write(version + "\n")
        temporary_path = Path(temporary.name)
    os.replace(temporary_path, path)
except OSError as error:
    raise SystemExit(f"cannot update {path}: {error}") from error
finally:
    if temporary_path is not None:
        temporary_path.unlink(missing_ok=True)

print(f"Ruby version pin updated to {version}.")
