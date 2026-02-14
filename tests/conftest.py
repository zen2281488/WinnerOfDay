import os
import pathlib
import sys


def pytest_configure():
    root = pathlib.Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    os.environ.setdefault("VK_TOKEN", "test-token")
