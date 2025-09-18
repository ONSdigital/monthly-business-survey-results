import importlib
import os
import shutil
from typing import Optional

from mbs_results import logger


def copy_script_and_config(
    package_name: str = "mbs_results", dest_dir: Optional[str] = None
) -> None:
    """
    Copy package_name/main.py and package_name/configs/config_user.json from
    the installed package identified by package_name into dest_dir (defaults to
    current working directory).

    This function is generic so other pipeline packages (for example cons_results)
    can call it as:
        copy_script_and_config(package_name="cons_results")

    Parameters
    ----------
    package_name : str, optional
        The name of the installed package to copy files from. Default is "mbs_results".
    dest_dir : str, optional
        The directory to copy the files to. If None, uses the current working directory.

    Raises
    ------
    ModuleNotFoundError
        If the requsted package is not importable.
    FileNotFoundError
        If the expected source files (main.py or configs/config_user.json)
        are not found in the package.

    Examples
    ---------
    >>> from mbs_results.utilities.copy_script_and_config import copy_script_and_config
    ...
    >>> copy_script_and_config()

    """

    try:
        pkg = importlib.import_module(package_name)
    except ModuleNotFoundError as e:
        logger.error(f"Package {package_name} not found. Ensure it is installed.")
        raise ModuleNotFoundError(
            f"Package {package_name} not found. Ensure it is installed."
        ) from e

    # Get the directory where mbs_results is installed
    pkg_dir = os.path.dirname(pkg.__file__)

    # Get main.py and config_user.json path
    main_path = os.path.join(pkg_dir, "main.py")
    config_user_path = os.path.join(pkg_dir, "configs", "config_user.json")

    missing = [p for p in (main_path, config_user_path) if not os.path.exists(p)]
    if missing:
        logger.error(
            f"Missing expected files in package {package_name}: {', '.join(missing)}"
        )
        raise FileNotFoundError(
            f"Expected files not found in package: {', '.join(missing)}"
        )

    # Get the destination for the copy
    dest_dir = dest_dir or os.getcwd()

    shutil.copy2(main_path, dest_dir)
    logger.info(f"{main_path} copied to {dest_dir}")
    shutil.copy2(config_user_path, dest_dir)
    logger.info(f"{config_user_path} copied to {dest_dir}")


if __name__ == "__main__":
    copy_script_and_config()
