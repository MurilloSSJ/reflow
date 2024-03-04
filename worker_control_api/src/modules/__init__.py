from os.path import dirname, isfile, join
import glob

modules = glob.glob(join(f"{dirname(__file__)}/*", "routes.py"))
modules = [
    f.split("/")[-2] for f in modules if isfile(f) and not f.endswith("__init__.py")
]
from . import *
