"""Winner of Day bot package."""


def create_app():
    from .main import create_app as _create_app

    return _create_app()


def run():
    from .main import run as _run

    return _run()


__all__ = ["create_app", "run"]
