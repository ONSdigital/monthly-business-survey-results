#!/usr/bin/env python
"""Pre commit hook to check for merge conflict flags in file."""
import argparse
import os.path
from typing import Optional, Sequence

CONFLICT_PATTERNS = [
    b"<<<<<<< ",
    b"======= ",
    b"=======\n",
    b">>>>>>> ",
]


def _is_in_merge() -> int:
    """Private function."""
    return os.path.exists(os.path.join(".git", "MERGE_MSG")) and (
        os.path.exists(os.path.join(".git", "MERGE_HEAD"))
        or os.path.exists(os.path.join(".git", "rebase-apply"))
        or os.path.exists(os.path.join(".git", "rebase-merge"))
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry function for script."""
    parser = argparse.ArgumentParser()
    parser.add_argument("filenames", nargs="*")
    parser.add_argument("--assume-in-merge", action="store_true")
    args = parser.parse_args(argv)

    if not _is_in_merge() and not args.assume_in_merge:
        return 0

    retcode = 0
    for filename in args.filenames:
        with open(filename, "rb") as inputfile:
            for i, line in enumerate(inputfile):
                for pattern in CONFLICT_PATTERNS:
                    if line.startswith(pattern):
                        print(
                            f'Merge conflict string "{pattern.decode()}" '
                            f"found in {filename}:{i + 1}",
                        )
                        retcode = 1

    return retcode


if __name__ == "__main__":
    exit(main())
