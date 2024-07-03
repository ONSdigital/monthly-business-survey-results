"""Setup script for creating package from code."""

from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="monthly-business-survey-results",
    version="0.0.1",
    description="Public Sector local Python downloads and preprocessing package",
    url="https://github.com/ONSdigital/monthly-business-survey-results",
    packages=find_packages(),
    package_data={"": ["*.toml", "*.r", "*.R", "*.pem"]},
    include_package_data=True,
    zip_safe=False,
    install_requires=requirements,
)
