import os
from pathlib import Path

from itseasy_pyutil.util import *

__version__ = "1.0.0"
with open(os.path.join(Path(__file__).parent.absolute(), "VERSION"), "r") as f:
    __version__ = f.read().strip().replace("\n", "")
