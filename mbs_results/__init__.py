"""
Package logger bootstrap.

- Exposes `logger` for package use, named after the package folder
  (mbs_results / cons_results).
- Buffers early records in a MemoryHandler until `configure_logger_with_run_id(config)`
  is called.
- When configured, attaches a FileHandler:
  - platform == "s3": writes to a local per-run temp file and uploads to S3
    on exit / on crash.
  - otherwise: writes directly into configured output_path (or package ../logs
    as fallback).
- Ensures every LogRecord has a `run_id` attribute via RunIDFilter so formatters
    may include it.
- Exposes module-level variables: `run_id` (str|None) and `LOG_FILE_PATH` (str|None).
"""

from __future__ import annotations

import atexit
import logging
import os
import sys
import tempfile
from datetime import datetime
from logging.handlers import MemoryHandler
from pathlib import Path
from typing import Dict, Optional
from uuid import uuid4

# Optional clients will be imported lazily
_boto3 = None
_raz_client = None
_s3_client = None

# Determine package / project name from package directory
_package_dir = Path(__file__).resolve().parent
_project_name = _package_dir.name  # 'mbs_results' or 'cons_results'.
_LOGGER_NAME = _project_name

logger = logging.getLogger(_LOGGER_NAME)
logger.setLevel(logging.DEBUG)
logger.propagate = False

# Run-time state filled by configure_logger_with_run_id
run_id: Optional[str] = None
LOG_FILE_PATH: Optional[str] = None
_S3_TARGET: Optional[Dict[str, str]] = None  # {"bucket":..., "key":...}

# Log format includes run_id (RunIDFilter guarantees attribute exists)
_LOG_FMT = (
    "[%(asctime)s: %(name)s: %(levelname)s: %(module)s: "
    "%(funcName)s: %(lineno)d] [run_id=%(run_id)s] %(message)s"
)
_formatter = logging.Formatter(_LOG_FMT)

# Memory buffer to capture early logs until file handler ready
_MEMORY_CAPACITY = 1000
_memory_handler = MemoryHandler(_MEMORY_CAPACITY, target=None)
_memory_handler.setFormatter(_formatter)

# Console handler
_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_formatter)
_console_handler.setLevel(logging.DEBUG)


class RunIDFilter(logging.Filter):
    """Ensure every LogRecord has run_id attribute for formatter safety."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = str(run_id) if run_id else "_"
        return True


# Attach filters and handlers to package logger
_memory_handler.addFilter(RunIDFilter())
_console_handler.addFilter(RunIDFilter())
logger.addHandler(_memory_handler)
logger.addHandler(_console_handler)


def _ensure_s3_client(config: Dict) -> Optional[object]:
    """
    Create and configure an S3 client now (not in atexit). Return client or None.
    """
    global _s3_client, _boto3, _raz_client
    import boto3 as _boto3
    import raz_client as _raz_client

    if _boto3 is None:
        logger.warning("boto3 not available; skipping S3 client creation")
        return None
    try:
        s3_client = _boto3.client("s3")
        ssl_file = config.get("ssl_file", "/etc/pki/tls/certs/ca-bundle.crt")
        if _raz_client:
            _raz_client.configure_ranger_raz(s3_client, ssl_file=ssl_file)
        _s3_client = s3_client
        return _s3_client
    except Exception:
        logger.exception("Failed to create S3 client during logger configuration")
        _s3_client = None
        return _s3_client


def _clean_root_logger_handlers():
    """Remove existing FileHandlers from root logger to avoid duplicates."""
    root_logger = logging.getLogger()
    seen_types = set()
    for handler in list(root_logger.handlers):
        if isinstance(handler, logging.FileHandler):
            handler_type = type(handler)
            if handler_type not in seen_types:
                seen_types.add(handler_type)
                root_logger.removeHandler(handler)
                handler.close()
            else:
                seen_types.add(handler_type)


def _setup_log_paths(config) -> tuple[str, Optional[Dict[str, str]]]:
    """Set up log file paths and S3 target.

    - For S3: use s3_bucket param.
    - For network: write directly into output_path or package ../logs.
    """
    platform = str(config.get("platform", "network")).lower()
    output_path = str(config.get("output_path", ""))
    s3_bucket = config.get("bucket")
    run_id = config.get("run_id")

    log_base_name = f"{_project_name}_{run_id}.log"

    if platform == "s3":
        local_dir = os.path.join(tempfile.gettempdir(), f"{_project_name}_logs", run_id)
        os.makedirs(local_dir, exist_ok=True)
        local_log_path = os.path.join(local_dir, log_base_name)
        s3_key = os.path.join(output_path, log_base_name)
        return local_log_path, {"bucket": s3_bucket, "key": s3_key}
    else:
        target_dir = output_path or os.path.join(_package_dir.parent, "logs")
        os.makedirs(target_dir, exist_ok=True)
        local_log_path = os.path.join(target_dir, f"{_LOGGER_NAME}_{run_id}.log")
        return local_log_path, None


def _upload_log_to_s3(config: Dict) -> None:
    """Upload log file to S3 (synchronous,
    use put_object to avoid thread-pools at shutdown).
    """
    if not LOG_FILE_PATH or not _S3_TARGET:
        return

    bucket = _S3_TARGET.get("bucket")
    key = _S3_TARGET.get("key")
    ssl_file = config.get("ssl_file")
    if not bucket or not key:
        logger.warning("Missing S3 target for log upload; skipping")
        return
    if _boto3 is None:
        logger.warning("boto3 not available; cannot upload log to S3.")
        return

    try:
        # create client now (or use pre-created one if you added caching)
        s3_client = _boto3.client("s3")
        # configure RAZ if available
        _raz_client.configure_ranger_raz(s3_client, ssl_file=ssl_file)

        logger.info(f"Uploading log {LOG_FILE_PATH} -> s3://{bucket}/{key}")
        # Use put_object (synchronous, no thread-pool)
        # to avoid interpreter-shutdown threading issues
        with open(LOG_FILE_PATH, "rb") as fh:
            s3_client.put_object(Bucket=bucket, Key=key, Body=fh)
        logger.info("Log upload to S3 complete")
    except Exception:
        logger.exception(f"Failed to upload log to s3://{bucket}/{key}")


def _flush_handlers():
    """Flush all handlers that support it."""
    for h in logger.handlers:
        try:
            if hasattr(h, "flush"):
                h.flush()
        except Exception:
            logger.exception("Failed during handlers flush to disk")


def _try_upload_logs(upload_func):
    """Attempt to upload logs if S3 target is configured."""
    try:
        if _S3_TARGET:
            upload_func()
    except Exception:
        logger.exception("Failed during upload-on-exception")


def _handle_uncaught_exception(exc_type, exc_value, exc_tb, upload_func):
    """Handle uncaught exceptions, log them, and upload logs if configured."""
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_tb)
        return
    try:
        logger.error("Uncaught Exception", exc_info=(exc_type, exc_value, exc_tb))
    except Exception:
        sys.__excepthook__(exc_type, exc_value, exc_tb)

    _flush_handlers()
    _try_upload_logs(upload_func)
    sys.__excepthook__(exc_type, exc_value, exc_tb)


def configure_logger_with_run_id(config: Dict) -> logging.Logger:
    """
    Configure the package logger once config and run_id are available.

    Parameters
    ----------
    config : dict
        Expected keys:
          - run_id (optional): if absent a new run_id is generated
          - platform: 'network' or 's3' (default 'network')
          - output_path: local path or s3://bucket/prefix (optional)
          - bucket: explicit S3 bucket name (optional)
          - ssl_file: path to SSL cert for raz_client if required (optional)

    Returns
    -------
    logging.Logger
        The configured package logger.
    """
    global run_id, LOG_FILE_PATH, _S3_TARGET

    if not config.get("run_id"):
        config["run_id"] = f"{datetime.now():%Y%m%d%H%M%S}-{uuid4().hex[:8]}"
    run_id = str(config["run_id"])

    _clean_root_logger_handlers()

    local_log_path, s3_target = _setup_log_paths(
        str(config.get("platform", "network")).lower(),
        str(config.get("output_path", "")),
        config.get("bucket"),
        run_id,
    )

    file_handler = logging.FileHandler(local_log_path, mode="a")
    file_handler.setFormatter(_formatter)
    file_handler.addFilter(RunIDFilter())

    try:
        _memory_handler.setTarget(file_handler)
        _memory_handler.flush()
    except Exception:
        logger.exception(
            f"Failed to flush MemoryHandler to FileHandler for run_id={run_id}"
        )
    finally:
        try:
            logger.removeHandler(_memory_handler)
            _memory_handler.close()
        except Exception:
            logger.exception(f"Failed to remove MemoryHandler for run_id={run_id}")

    logger.addHandler(file_handler)

    LOG_FILE_PATH = str(local_log_path) if local_log_path else None
    _S3_TARGET = s3_target

    logger.info(f"Logger configured for run_id={run_id}; file={LOG_FILE_PATH}")

    if _S3_TARGET:

        # create and cache the S3 client now
        # so upload handler does not import during shutdown
        _ensure_s3_client(config)

        def upload_func():
            return _upload_log_to_s3(config)

        atexit.register(upload_func)

        def exception_handler(t, v, tb):
            return _handle_uncaught_exception(t, v, tb, upload_func)

        sys.excepthook = exception_handler

    return logger
