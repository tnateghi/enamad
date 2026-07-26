#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Portable task scheduler for the Enamad project (Laravel-scheduler style).

Runs three recurring scrape jobs via extract_enamad.py:
  1. --update              : fetch newly-added domains (first N list pages, captcha)
  2. --refresh-stale       : fill missing contact info (trust seal, no captcha)
  3. --refresh-stale       : weekly touch-up for domains with complete info

Frequencies are configurable via config.ini ([scheduler] section) or SCHED_* env vars.

Run:
  python scheduler.py
  python scheduler.py --config path/to/config.ini
"""

from __future__ import annotations

import argparse
import configparser
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[2]
EXTRACT_SCRIPT = SCRIPT_DIR.parent / "scraper" / "extract_enamad.py"
DEFAULT_CONFIG = REPO_ROOT / "config.ini"

from logging_setup import setup_logging

setup_logging()
log = logging.getLogger("enamad-scheduler")


def _env(*keys: str) -> str | None:
    for key in keys:
        value = os.environ.get(key)
        if value is not None and value.strip() != "":
            return value.strip()
    return None


@dataclass(frozen=True)
class SchedulerConfig:
    timezone: str
    update_cron: str
    update_pages: int
    update_workers: int
    update_chunk_pages: int
    refresh_missing_cron: str
    refresh_missing_limit: int
    refresh_full_cron: str
    refresh_full_days: int
    refresh_full_limit: int
    refresh_workers: int
    refresh_newest_first: bool
    run_on_start: bool
    enable_update: bool
    enable_refresh_missing: bool
    enable_refresh_full: bool
    enable_automation_flush: bool
    automation_flush_cron: str


def load_scheduler_config(path: Path) -> SchedulerConfig:
    parser = configparser.ConfigParser()
    if path.is_file():
        parser.read(path, encoding="utf-8")

    def get(key: str, fallback: str) -> str:
        env_key = f"SCHED_{key.upper()}"
        return _env(env_key) or parser.get("scheduler", key, fallback=fallback)

    def get_int(key: str, fallback: int) -> int:
        return int(get(key, str(fallback)))

    def get_bool(key: str, fallback: bool) -> bool:
        return get(key, "yes" if fallback else "no").lower() in ("1", "true", "yes", "on")

    # Legacy SCHED_REFRESH_CRON still maps to the missing-details job.
    refresh_missing_cron = (
        _env("SCHED_REFRESH_MISSING_CRON")
        or get("refresh_missing_cron", "")
        or get("refresh_cron", "30 */6 * * *")
    )
    refresh_missing_limit = get_int(
        "refresh_missing_limit",
        get_int("refresh_limit", 400),
    )

    return SchedulerConfig(
        timezone=get("timezone", "Asia/Tehran"),
        update_cron=get("update_cron", "0 */4 * * *"),
        update_pages=get_int("update_pages", 50),
        update_workers=get_int("update_workers", 1),
        update_chunk_pages=get_int("update_chunk_pages", 10),
        refresh_missing_cron=refresh_missing_cron,
        refresh_missing_limit=refresh_missing_limit,
        refresh_full_cron=get("refresh_full_cron", "0 4 * * 0"),
        refresh_full_days=get_int("refresh_full_days", 7),
        refresh_full_limit=get_int("refresh_full_limit", 500),
        refresh_workers=get_int("refresh_workers", 4),
        refresh_newest_first=get_bool("refresh_newest_first", True),
        run_on_start=get_bool("run_on_start", True),
        enable_update=get_bool("enable_update", True),
        enable_refresh_missing=get_bool(
            "enable_refresh_missing",
            get_bool("enable_refresh", True),
        ),
        enable_refresh_full=get_bool("enable_refresh_full", True),
        enable_automation_flush=get_bool("enable_automation_flush", True),
        automation_flush_cron=get("automation_flush_cron", "*/10 * * * *"),
    )


def _ensure_import_paths() -> None:
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    if str(REPO_ROOT / "src") not in sys.path:
        sys.path.insert(0, str(REPO_ROOT / "src"))


def wait_for_mysql(config_path: Path, timeout_sec: int = 300, interval_sec: float = 5.0) -> bool:
    """Block until MySQL accepts connections (common after host/container reboot)."""
    _ensure_import_paths()
    from db import load_config, mysql_connection

    deadline = time.monotonic() + timeout_sec
    attempt = 0
    while True:
        attempt += 1
        try:
            cfg = load_config(config_path)
            with mysql_connection(cfg.mysql) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            log.info("MySQL is ready (attempt %d).", attempt)
            return True
        except Exception as exc:
            if time.monotonic() >= deadline:
                log.error("MySQL not ready after %ds: %s", timeout_sec, exc)
                return False
            log.warning("Waiting for MySQL (attempt %d): %s", attempt, exc)
            time.sleep(interval_sec)


def _run(label: str, extra_args: list[str], config_path: Path) -> None:
    cmd = [sys.executable, str(EXTRACT_SCRIPT), *extra_args,
           "--config", str(config_path)]
    log.info("Running job '%s': %s", label, " ".join(extra_args))
    try:
        result = subprocess.run(cmd, cwd=str(REPO_ROOT))
        if result.returncode == 0:
            log.info("Job '%s' finished successfully.", label)
        else:
            log.warning("Job '%s' exited with code %s.", label, result.returncode)
    except Exception as exc:
        log.error("Job '%s' failed to run: %s", label, exc)


def make_update_job(cfg: SchedulerConfig, config_path: Path):
    def job() -> None:
        args = [
            "--update",
            "--update-pages", str(cfg.update_pages),
            "--workers", str(cfg.update_workers),
            "--chunk-pages", str(cfg.update_chunk_pages),
        ]
        _run("update", args, config_path)

    return job


def make_refresh_missing_job(cfg: SchedulerConfig, config_path: Path):
    """Refresh domains that lack address/phone/email (priority)."""

    def job() -> None:
        args = [
            "--refresh-stale",
            "--stale-days", "0",
            "--refresh-limit", str(cfg.refresh_missing_limit),
            "--refresh-workers", str(cfg.refresh_workers),
            "--delay", "0",
            "--missing-only",
        ]
        if cfg.refresh_newest_first:
            args.append("--newest-first")
        _run("refresh-missing", args, config_path)

    return job


def make_refresh_full_job(cfg: SchedulerConfig, config_path: Path):
    """Weekly refresh for domains whose details are already complete."""

    def job() -> None:
        args = [
            "--refresh-stale",
            "--stale-days", str(cfg.refresh_full_days),
            "--refresh-limit", str(cfg.refresh_full_limit),
            "--refresh-workers", str(cfg.refresh_workers),
            "--delay", "0",
        ]
        if cfg.refresh_newest_first:
            args.append("--newest-first")
        _run("refresh-full", args, config_path)

    return job


def make_automation_flush_job(config_path: Path):
    """Flush SMS automations that were queued outside their send window."""

    def job() -> None:
        try:
            _ensure_import_paths()

            from db import load_config, mysql_connection
            from crm_db import ensure_crm_tables
            from crm_service import process_pending_automations

            cfg = load_config(config_path)
            with mysql_connection(cfg.mysql) as conn:
                ensure_crm_tables(conn)
                n = process_pending_automations(conn)
            log.info("Automation flush processed %s queued row(s).", n)
        except Exception as exc:  # noqa: BLE001
            log.exception("Automation flush failed: %s", exc)

    return job


def main() -> int:
    argp = argparse.ArgumentParser(description="Enamad recurring task scheduler")
    argp.add_argument("--config", default=str(DEFAULT_CONFIG), help="Path to config.ini")
    parsed = argp.parse_args()

    config_path = Path(parsed.config)
    if not config_path.is_absolute():
        config_path = SCRIPT_DIR / config_path
        if not config_path.is_file():
            alt = SCRIPT_DIR.parents[2] / parsed.config
            if alt.is_file():
                config_path = alt

    cfg = load_scheduler_config(config_path)

    if not wait_for_mysql(config_path):
        return 1

    scheduler = BlockingScheduler(timezone=cfg.timezone)

    update_job = make_update_job(cfg, config_path)
    refresh_missing_job = make_refresh_missing_job(cfg, config_path)
    refresh_full_job = make_refresh_full_job(cfg, config_path)
    automation_flush_job = make_automation_flush_job(config_path)

    if cfg.enable_update:
        scheduler.add_job(
            update_job,
            CronTrigger.from_crontab(cfg.update_cron, timezone=cfg.timezone),
            id="update",
            name="Fetch new domains",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        log.info("Scheduled 'update' with cron '%s' (%s).", cfg.update_cron, cfg.timezone)

    if cfg.enable_refresh_missing:
        scheduler.add_job(
            refresh_missing_job,
            CronTrigger.from_crontab(cfg.refresh_missing_cron, timezone=cfg.timezone),
            id="refresh-missing",
            name="Refresh domains missing contact info",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        log.info(
            "Scheduled 'refresh-missing' with cron '%s' (%s), limit %d.",
            cfg.refresh_missing_cron,
            cfg.timezone,
            cfg.refresh_missing_limit,
        )

    if cfg.enable_refresh_full:
        scheduler.add_job(
            refresh_full_job,
            CronTrigger.from_crontab(cfg.refresh_full_cron, timezone=cfg.timezone),
            id="refresh-full",
            name="Weekly refresh of complete domains",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=7200,
        )
        log.info(
            "Scheduled 'refresh-full' with cron '%s' (%s), older than %d days, limit %d.",
            cfg.refresh_full_cron,
            cfg.timezone,
            cfg.refresh_full_days,
            cfg.refresh_full_limit,
        )

    if cfg.enable_automation_flush:
        scheduler.add_job(
            automation_flush_job,
            CronTrigger.from_crontab(cfg.automation_flush_cron, timezone=cfg.timezone),
            id="automation-flush",
            name="Flush queued automation SMS",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=600,
        )
        log.info(
            "Scheduled 'automation-flush' with cron '%s' (%s).",
            cfg.automation_flush_cron,
            cfg.timezone,
        )

    if not scheduler.get_jobs():
        log.error("No jobs enabled. Set enable_update/enable_refresh in [scheduler].")
        return 1

    if cfg.run_on_start:
        log.info("run_on_start enabled — running priority jobs now (update first).")
        if cfg.enable_automation_flush:
            automation_flush_job()
        if cfg.enable_update:
            update_job()
        if cfg.enable_refresh_missing:
            refresh_missing_job()

    log.info("Scheduler started. Press Ctrl+C to exit.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
