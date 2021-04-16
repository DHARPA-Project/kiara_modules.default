# -*- coding: utf-8 -*-
import os
import sys
from appdirs import AppDirs

kiara_modules_default_app_dirs = AppDirs("kiara_modules_default", "DHARPA")

if not hasattr(sys, "frozen"):
    KIARA_MODULES_DEFAULT_MODULE_BASE_FOLDER = os.path.dirname(__file__)
    """Marker to indicate the base folder for the `kiara_modules_default` module."""
else:
    KIARA_MODULES_DEFAULT_MODULE_BASE_FOLDER = os.path.join(
        sys._MEIPASS, "kiara_modules_default"  # type: ignore
    )
    """Marker to indicate the base folder for the `kiara_modules_default` module."""

KIARA_MODULES_DEFAULT_RESOURCES_FOLDER = os.path.join(
    KIARA_MODULES_DEFAULT_MODULE_BASE_FOLDER, "resources"
)
"""Default resources folder for this package."""

KIARA_MODULES_DEFAULT_PIPELINES_FOLDER = os.path.join(
    KIARA_MODULES_DEFAULT_RESOURCES_FOLDER, "pipelines"
)
