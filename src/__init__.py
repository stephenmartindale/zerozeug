from os import listdir
from os.path import dirname
from importlib import import_module

for module in listdir(dirname(__file__)):
    if (module != '__init__.py') and (module[-3:] == '.py'):
        import_module("." + module[:-3], __name__)

del module

del listdir
del dirname
del import_module
