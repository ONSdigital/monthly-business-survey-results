"""Setup script for creating package from code."""

from setuptools import find_packages, setup

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

with open("requirements-dev.txt") as f:
    requirements_dev = f.read().splitlines()

setup(
    name="monthly-business-survey-results",
    version="0.1.0",
    description="Public Sector local Python downloads and preprocessing package",
    url="https://github.com/ONSdigital/monthly-business-survey-results",
    packages=find_packages(),
    package_data={"": ["*.toml", "*.r", "*.R", "*.pem"]},
    include_package_data=True,
    zip_safe=False,
    install_requires=requirements,
    extras_require={"dev": requirements_dev},
)
