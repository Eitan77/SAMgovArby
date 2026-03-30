"""Output filtering wrapper for SAMgovArby pipeline scripts.

Reduces output verbosity by:
- Deduplicating identical log lines (shows count instead)
- Stripping timestamps and level indicators (--summary mode)
- Aggregating progress into single-line format
- Filtering to errors only (--errors mode)

Usage:
    python run_quiet.py build_training_set.py
    python run_quiet.py --errors backtest.py
    python run_quiet.py --summary optimizer.py
    python run_quiet.py --stats backtest.py --quiet
"""

import argparse
import re
import subprocess
import sys
from collections import defaultdict
from typing import Optional


class OutputFilter:
    """Filters and deduplicates log output."""

    def __init__(self, errors_only: bool = False, summary: bool = False, stats: bool = False):
        self.errors_only = errors_only
        self.summary = summary
        self.stats = stats
        self.line_counts = defaultdict(int)
        self.last_line = None
        self.error_lines = []
        self.summary_lines = []

    def is_error_line(self, line: str) -> bool:
        """Check if line is an error/warning."""
        return any(
            keyword in line.upper()
            for keyword in ["ERROR", "WARNING", "FAILED", "EXCEPTION", "TRACEBACK"]
        )

    def is_summary_line(self, line: str) -> bool:
        """Check if line looks like a summary (table, final results, etc.)."""
        return any(
            pattern in line
            for pattern in ["===", "---", "│", "├", "└", "Summary", "Results", "Total"]
        )

    def strip_timestamp(self, line: str) -> str:
        """Remove timestamp and log level from line."""
        # Remove ISO timestamps (YYYY-MM-DD HH:MM:SS)
        line = re.sub(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", "", line)
        # Remove log level markers
        line = re.sub(r"\[(?:DEBUG|INFO|WARNING|ERROR|CRITICAL)\]", "", line)
        return line.strip()

    def process_line(self, line: str) -> Optional[str]:
        """Process and filter a line. Returns the line to print, or None to skip."""

        line = line.rstrip("\n")

        if self.errors_only and not self.is_error_line(line):
            return None

        if self.stats:
            if self.is_summary_line(line):
                return line
            if self.is_error_line(line):
                return line
            return None

        if self.summary:
            if self.is_summary_line(line):
                return line
            if self.is_error_line(line):
                return line
            return None

        # Deduplication: track identical lines
        clean_line = self.strip_timestamp(line) if not self.summary else line
        self.line_counts[clean_line] += 1

        # Only print first occurrence of a line
        if clean_line != self.last_line:
            self.last_line = clean_line
            return line

        return None  # Duplicate, skip

    def flush_deduped(self):
        """Flush any remaining deduped lines with counts."""
        if not self.summary and not self.errors_only and not self.stats:
            for line, count in sorted(self.line_counts.items()):
                if count > 1:
                    print(f"{line} [×{count}]")


def run_with_filter(cmd: list[str], filter_args: dict):
    """Run command and filter output."""
    output_filter = OutputFilter(**filter_args)

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        for line in proc.stdout:
            filtered = output_filter.process_line(line)
            if filtered:
                print(filtered, end="\n")

        proc.wait()
        output_filter.flush_deduped()

        return proc.returncode

    except Exception as e:
        print(f"Error running command: {e}", file=sys.stderr)
        return 1


def main():
    parser = argparse.ArgumentParser(
        description="Run pipeline scripts with output filtering",
        epilog="Example: python run_quiet.py --errors backtest.py --quiet",
    )

    parser.add_argument(
        "command",
        nargs="+",
        help="Command to run (e.g., 'build_training_set.py' or 'python backtest.py')",
    )

    parser.add_argument(
        "--errors",
        action="store_true",
        help="Show only errors and warnings",
    )

    parser.add_argument(
        "--summary",
        action="store_true",
        help="Show only summary/table output and errors",
    )

    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show only stats, summaries, and errors",
    )

    args, unknown = parser.parse_known_args()

    # Build full command (add 'python' if script name is provided without 'python')
    cmd = args.command
    if cmd[0].endswith(".py") and not cmd[0].startswith("python"):
        cmd = ["python"] + cmd

    # Append any remaining args
    cmd.extend(unknown)

    filter_args = {
        "errors_only": args.errors,
        "summary": args.summary,
        "stats": args.stats,
    }

    return run_with_filter(cmd, filter_args)


if __name__ == "__main__":
    sys.exit(main())
