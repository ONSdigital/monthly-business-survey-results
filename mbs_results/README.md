# MBS CLI Quick Reference

This repository for MBS (Monthly Business Suvery) Results provides console entry point (installed as scripts) to run pipeline
task and to copy runnable project files into a working folder.

## Install (development)
- From the repo root (Windows Powershell or CDP Terminal):
    ```bash
    pip install -e .
    ```
    - This registers the console scripts below.

## Available Console Script
- `setup_mbs`
    - **Purpose:** Copy runnable files into your current working directory so you can run the pipeline without editing package sources.
    - **What is Copies:** `main.py`, `configs/config_user.json`, and the package `README.md` (if present).
    - **When to use:** First time you want local copies of the scripts/config tp adapt or run the pipeline.
    - Example (Powershell and bash):
        ```bash
        setup_mbs
        ```
- `run_mbs_main`
    - **Purpose:** Run the main MBS pipeline (end-to-end: staging → imputation → estimation → outlier detection → outputs).
    - **Inputs:** `configs/config_user.json` (the local copy created by `setup_mbs`) and snapshot/data files referenced in that config.
    - **Example:**
        ```bash
        run_mbs_main
        ```
    - **How to run MBS:** Step-by-step on how to run MBS is documented in the [MBS Configuration COnfluence page.](https://officefornationalstatistics.atlassian.net/wiki/spaces/MC/pages/59643308/MBS+Configuration).

- `run_se_period_zero`
    - **Purpose:** Run the period_zero selective editing routine (pipeline-specific helper).
    - **When to use:** For running start of period processing when IDBR files have been provided, produces selective editing contributor and responses files for SPP.
    - **Example:**
        ```bash
        run_se_period_zero
        ```

- `run_final_output`
    - **Purpose:** Produce final output artifacts used by downstream systems or publication (packaging /copying final CSVs etc.).
    - **Example:**
        ```bash
        run_final_output
        ```

- `additional_outputs`
    - **Purpose:** Produce addtional QA / analysis outputs (selective editing outputs, turnover analysis, devolved nations outputs, other auxiliary outputs).
    - **Examples:**
        ```bash
        additional_outputs
        ```

## Notes & tips
- Ensure you run the console scripts from the directory where you want the output files and where `config_user.json` will be located.
- `setup_mbs` is safe to re-run; it will copy recent package `main.py` and config into the current directory (it may overwrite existing files).
- For other pipeline packages that reuse these utilities (for example `cons_results`), we provide the same helper; there are wrapper entry points you can add to that package so `setup_cons` can be available.
- If you prefer to run programmatically:
  - from Python: from mbs_results.main import run_mbs_main; run_mbs_main()
- Troubleshooting:
  - If a console script is not found after install, ensure your virtual environment's Scripts folder is on PATH or that the package was installed into the active environment.

Contact / maintainers
- See [infomation the confluence](https://officefornationalstatistics.atlassian.net/wiki/spaces/MC/pages/59643314/Introduction) for details.
