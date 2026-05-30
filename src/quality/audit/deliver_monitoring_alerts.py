import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--process-date",
        default=datetime.now(UTC).date().isoformat(),
        help="Monitoring process date, format YYYY-MM-DD",
    )
    parser.add_argument(
        "--report-path",
        default=None,
        help="Monitoring report JSON path. Defaults to data/monitoring/monitoring_report_<date>.json",
    )
    parser.add_argument(
        "--history-path",
        default="data/monitoring/alert_history.jsonl",
        help="Append-only alert history JSONL path.",
    )
    parser.add_argument(
        "--summary-path",
        default=None,
        help="Latest alert summary JSON path. Defaults to data/monitoring/alert_summary_<date>.json",
    )
    parser.add_argument(
        "--channel",
        default="local_file",
        help="Logical alert delivery channel name for this local demo.",
    )
    return parser.parse_args()


def read_report(report_path):
    if not report_path.exists():
        raise SystemExit(f"Monitoring report not found: {report_path}")
    return json.loads(report_path.read_text(encoding="utf-8"))


def build_alert_id(process_date, finding):
    raw_id = "|".join(
        [
            process_date,
            str(finding.get("severity")),
            str(finding.get("rule_id")),
            str(finding.get("message")),
        ]
    )
    return hashlib.sha256(raw_id.encode("utf-8")).hexdigest()[:16]


def build_alerts(report, channel):
    process_date = report["process_date"]
    emitted_at = datetime.now(UTC).isoformat()
    alerts = []

    for finding in report.get("findings", []):
        alerts.append(
            {
                "alert_id": build_alert_id(process_date, finding),
                "process_date": process_date,
                "emitted_at": emitted_at,
                "channel": channel,
                "monitoring_status": report.get("status"),
                "severity": finding.get("severity"),
                "rule_id": finding.get("rule_id"),
                "message": finding.get("message"),
                "value": finding.get("value"),
                "threshold": finding.get("threshold"),
            }
        )

    return alerts


def append_alert_history(history_path, alerts):
    history_path.parent.mkdir(parents=True, exist_ok=True)
    if not alerts:
        return

    with history_path.open("a", encoding="utf-8") as history_file:
        for alert in alerts:
            history_file.write(json.dumps(alert) + "\n")


def build_summary(report, alerts, channel, report_path, history_path):
    critical_count = sum(1 for alert in alerts if alert.get("severity") == "critical")
    warning_count = sum(1 for alert in alerts if alert.get("severity") == "warning")
    return {
        "process_date": report["process_date"],
        "generated_at": datetime.now(UTC).isoformat(),
        "channel": channel,
        "monitoring_status": report.get("status"),
        "alert_count": len(alerts),
        "critical_count": critical_count,
        "warning_count": warning_count,
        "report_path": str(report_path),
        "history_path": str(history_path),
        "alerts": alerts,
    }


def main():
    args = parse_args()
    process_date = args.process_date
    report_path = Path(
        args.report_path or f"data/monitoring/monitoring_report_{process_date}.json"
    )
    history_path = Path(args.history_path)
    summary_path = Path(
        args.summary_path or f"data/monitoring/alert_summary_{process_date}.json"
    )

    report = read_report(report_path)
    alerts = build_alerts(report, args.channel)
    append_alert_history(history_path, alerts)

    summary = build_summary(report, alerts, args.channel, report_path, history_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("Monitoring alerts delivered")
    print(f"Process date: {process_date}")
    print(f"Channel: {args.channel}")
    print(f"Monitoring status: {summary['monitoring_status']}")
    print(f"Alerts delivered: {summary['alert_count']}")
    print(f"Critical alerts: {summary['critical_count']}")
    print(f"Warning alerts: {summary['warning_count']}")
    print(f"History: {history_path.resolve()}")
    print(f"Summary: {summary_path.resolve()}")


if __name__ == "__main__":
    main()
