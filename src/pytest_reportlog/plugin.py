import json
import logging
from random import randint
from typing import Dict, Any, TextIO

from _pytest.pathlib import Path

import pytest
import requests
import time


def send_report_to_dataset(url: str, token: str, report_json: dict) -> requests.Response:
    headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
    }
    add_events_url = f"{url}/services/collector/event"
    response = requests.post(add_events_url, headers=headers, json=report_json)
    response.raise_for_status()
    return response


def pytest_addoption(parser):
    group = parser.getgroup("terminal reporting", "report-log plugin options")
    group.addoption(
        "--report-log",
        action="store",
        metavar="path",
        default="no.log",
        help="Path to line-based json objects of test session events.",
    )
    group.addoption(
        "--report-log-exclude-logs-on-passed-tests",
        action="store_true",
        default=False,
        help="Don't capture logs for passing tests",
    )
    group.addoption(
        "--report-log-dataset",
        action="store_true",
        default=False,
        help="Send report json to DataSet instead of writing to file",
    )
    group.addoption(
        "--report-log-dataset-url",
        action="store",
        default=None,
        help="URL to DataSet API",
    )
    group.addoption(
        "--report-log-dataset-token",
        action="store",
        default=None,
        help="Token for DataSet API",
    )


def pytest_configure(config):
    report_log = config.option.report_log
    if report_log and not hasattr(config, "workerinput"):
        config._report_log_plugin = ReportLogPlugin(config, Path(report_log))
        config.pluginmanager.register(config._report_log_plugin)
    if config.option.report_log_dataset:
        config._report_log_dataset = True
        config._report_log_plugin = ReportLogPlugin(config, Path(report_log))
        config.pluginmanager.register(config._report_log_plugin)
    if config.option.report_log_dataset_url:
        config._report_log_dataset_url = config.option.report_log_dataset_url
    if config.option.report_log_dataset_token:
        config._report_log_dataset_token = config.option.report_log_dataset_token


def pytest_unconfigure(config):
    report_log_plugin = getattr(config, "_report_log_plugin", None)
    if report_log_plugin:
        report_log_plugin.close()
        del config._report_log_plugin


def _open_filtered_writer(log_path: Path) -> TextIO:
    if log_path.suffix == ".gz":
        import gzip

        return gzip.open(log_path, "wt", encoding="UTF-8")
    elif log_path.suffix == ".bz2":
        import bz2

        return bz2.open(log_path, "wt", encoding="UTF-8")
    elif log_path.suffix == ".xz":
        import lzma

        return lzma.open(log_path, "wt", encoding="UTF-8")
    else:
        # line buffer for text mode to ease tail -f
        return log_path.open("wt", buffering=1, encoding="UTF-8")


class ReportLogPlugin:
    def __init__(self, config, log_path: Path = None):
        self._config = config
        if log_path:
            self._log_path = log_path

            log_path.parent.mkdir(parents=True, exist_ok=True)
            self._file = _open_filtered_writer(log_path)
        else:
            self._file = None
        self._unique_id = f"test_report_{time.time_ns()}_{randint(100000, 999999)}"
        logging.info(f"Report log unique id: {self._unique_id}")

    def close(self):
        if self._file is not None:
            self._file.close()
            self._file = None

    def persist_data(self, data):
        data["unique_id"] = self._unique_id
        data["parser"] = "pytest-reportlog"
        try:
            json_data = json.dumps(data)
        except TypeError:
            data = cleanup_unserializable(data)
            json_data = json.dumps(data)
        if self._file is not "no.log":
            self._file.write(json_data + "\n")
            self._file.flush()
        if self._config._report_log_dataset:
            send_report_to_dataset(
                self._config._report_log_dataset_url, self._config._report_log_dataset_token, data)

    def pytest_sessionstart(self):
        data = {"pytest_version": pytest.__version__, "$report_type": "SessionStart"}
        self.persist_data(data)

    def pytest_sessionfinish(self, exitstatus):
        data = {"exitstatus": exitstatus, "$report_type": "SessionFinish"}
        self.persist_data(data)

    def pytest_runtest_logreport(self, report):
        data = self._config.hook.pytest_report_to_serializable(
            config=self._config, report=report
        )
        if (
            self._config.option.report_log_exclude_logs_on_passed_tests
            and data.get("outcome", "") == "passed"
        ):
            data["sections"] = [
                s
                for s in data["sections"]
                if s[0]
                not in [
                    "Captured log setup",
                    "Captured log call",
                    "Captured log teardown",
                ]
            ]

        self.persist_data(data)

    def pytest_warning_recorded(self, warning_message, when, nodeid, location):
        data = {
            "category": (
                warning_message.category.__name__ if warning_message.category else None
            ),
            "filename": warning_message.filename,
            "lineno": warning_message.lineno,
            "message": warning_message.message,
        }
        extra_data = {
            "$report_type": "WarningMessage",
            "when": when,
            "location": location,
        }
        data.update(extra_data)
        self.persist_data(data)

    def pytest_collectreport(self, report):
        data = self._config.hook.pytest_report_to_serializable(
            config=self._config, report=report
        )
        self.persist_data(data)

    def pytest_terminal_summary(self, terminalreporter):
        if self._file is not "no.log":
            terminalreporter.write_sep("-", f"generated report log file: {self._log_path}")
        if self._config._report_log_dataset:
            terminalreporter.write_sep("-", f"Generated test log events with unique id: {self._unique_id}")


def cleanup_unserializable(d: Dict[str, Any]) -> Dict[str, Any]:
    """Return new dict with entries that are not json serializable by their str()."""
    result = {}
    for k, v in d.items():
        try:
            json.dumps({k: v})
        except TypeError:
            v = str(v)
        result[k] = v
    return result
