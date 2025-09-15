# `mbs_results` package overview

All functions for this project, should be stored in this folder. All tests should be
stored in the `tests` folder, which is one-level above this folder in the main project
directory.

Sub-folders can be added as needed.

It is strongly suggested that you import functions in the `mbs_results/__init__.py` script. You
should also try to use absolute imports in this script whenever possible. Relative
imports are not discouraged, but can be an issue for projects where the directory
structure is likely to change. See [PEP 328 for details on absolute imports][pep-328].

[pep-328]: https://www.python.org/dev/peps/pep-0328/
