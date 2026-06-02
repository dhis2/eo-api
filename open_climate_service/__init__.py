try:
    from importlib.metadata import version as _get_version

    __version__ = _get_version("open-climate-service")
except Exception:
    __version__ = "unknown"
