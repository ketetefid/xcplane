# SPDX-License-Identifier: GPL-3.0-or-later

from ansible.plugins.callback import CallbackBase

class CallbackModule(CallbackBase):
    """
    Quiet callback for xcplane:
    - Successful tasks produce one line
    - Skipped tasks are optionally shown
    - Failed and unreachable tasks show useful diagnostics
    """

    CALLBACK_VERSION = 2.0
    CALLBACK_TYPE = "stdout"
    CALLBACK_NAME = "quiet_success"

    def _task_name(self, result):
        return result._task.get_name()

    def _host_name(self, result):
        return result._host.get_name()

    def _display_error_output(self, result):
        data = result._result

        for key in ("stdout", "stderr"):
            value = data.get(key)
            if not value:
                continue

            self._display.display(f"  {key}:", color="red")

            for line in value.splitlines():
                self._display.display(f"    {line}", color="red")

    def v2_runner_on_ok(self, result):
        host = self._host_name(result)
        task = self._task_name(result)

        status = "changed" if result._result.get("changed", False) else "ok"

        self._display.display(
            f"✓ [{host}] {task} ({status})",
            color="green",
        )

    def v2_runner_on_skipped(self, result):
        host = self._host_name(result)
        task = self._task_name(result)

        self._display.display(
            f"⊘ [{host}] {task} (skipped)",
            color="yellow",
        )

    def v2_runner_on_failed(self, result, ignore_errors=False):
        host = self._host_name(result)
        task = self._task_name(result)

        status = "FAILED (ignored)" if ignore_errors else "FAILED"

        self._display.display(
            f"✗ [{host}] {task} {status}",
            color="red",
        )

        msg = result._result.get("msg")
        if msg:
            self._display.display(
                f"  Error: {msg}",
                color="red",
            )

        self._display_error_output(result)

    def v2_runner_on_unreachable(self, result):
        host = self._host_name(result)
        task = self._task_name(result)

        self._display.display(
            f"✗ [{host}] {task} UNREACHABLE",
            color="red",
        )

        msg = result._result.get("msg", "Host unreachable")

        self._display.display(
            f"  Error: {msg}",
            color="red",
        )

    def v2_playbook_on_stats(self, stats):
        self._display.banner("PLAY RECAP")

        for host in sorted(stats.processed):
            summary = stats.summarize(host)

            failed = summary["failures"]
            unreachable = summary["unreachable"]

            color = "red" if (failed or unreachable) else "green"

            self._display.display(
                f"{host:30} : "
                f"ok={summary['ok']}  "
                f"changed={summary['changed']}  "
                f"unreachable={unreachable}  "
                f"failed={failed}",
                color=color,
            )
