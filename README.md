# `monthly-business-survey-results`

Reproducible Analytical Pipeline (RAP) for the Monthly Business Survey.

## Getting started

To start using this project, first make sure your system meets the below requirements.

### Requirements

To install the Python requirements, open your terminal and enter:

```shell
pip install .
```
or for installing in development mode:

```shell
pip install .[dev]
```

### Running the pipeline

Once the module has been installed copy over the config and main script by running

```shell
setup_mbs
```
Following this you can either run the main.py script, or use the command line argument

```shell
run_mbs_main
```
To run the main mbs pipeline. This will load the local config you copied over, so
populate this with the required filepaths.

## Licence

Unless stated otherwise, the codebase is released under the MIT License. This covers
both the codebase and any sample code in the documentation. The documentation is ©
Crown copyright and available under the terms of the Open Government 3.0 licence.

## Contributing

[If you want to help us build, and improve `monthly-business-survey-results`, view our
contributing guidelines][contributing].

## Acknowledgements

[This project structure is based on the `govcookiecutter` template
project][govcookiecutter].

[contributing]: ./docs/source/project_docs_md/contributor_guide/CONTRIBUTING.md
[govcookiecutter]: https://github.com/best-practice-and-impact/govcookiecutter
[docs-loading-environment-variables]: ./docs/user_guide/loading_environment_variables.md
[docs-loading-environment-variables-secrets]: ./docs/user_guide/loading_environment_variables.md#storing-secrets-and-credentials
