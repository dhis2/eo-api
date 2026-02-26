try:
    from importlib.metadata import version as _get_version

    __version__ = _get_version("eo-api")
except Exception:
    __version__ = "unknown"
