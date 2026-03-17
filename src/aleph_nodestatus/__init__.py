# -*- coding: utf-8 -*-
try:
    from importlib.metadata import version, PackageNotFoundError
    dist_name = "aleph-nodestatus"
    __version__ = version(dist_name)
except PackageNotFoundError:
    __version__ = "unknown"
