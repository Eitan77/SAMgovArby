"""
SAMgovArby Desktop GUI
Run: python gui.py
Requires: pip install PyQt6 matplotlib
"""

import csv
import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import matplotlib
matplotlib.use("QtAgg")
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg
from matplotlib.figure import Figure

from PyQt6.QtCore import (
    QAbstractTableModel, QDate, QFileSystemWatcher, QModelIndex,
    QProcess, QProcessEnvironment, QSortFilterProxyModel, Qt, QTimer,
    pyqtSignal,
)
from PyQt6.QtGui import QColor, QFont, QPalette
from PyQt6.QtWidgets import (
    QApplication, QCheckBox, QComboBox, QDateEdit, QDoubleSpinBox,
    QFileDialog, QFormLayout, QGroupBox, QHBoxLayout, QHeaderView,
    QLabel, QLineEdit, QMainWindow, QMessageBox, QPlainTextEdit,
    QProgressBar, QPushButton, QScrollArea, QSizePolicy, QSpinBox,
    QSplitter, QStatusBar, QTabWidget, QTableView, QVBoxLayout,
    QWidget,
)

# ─── Constants ────────────────────────────────────────────────────────────────

SCRIPTS_DIR  = Path(__file__).parent
DATASETS_DIR = SCRIPTS_DIR / "datasets"
CONFIG_PATH  = SCRIPTS_DIR / "config.py"

OUTPUT_FILES = {
    "signal_log":   SCRIPTS_DIR / "signal_log.csv",
    "positions":    SCRIPTS_DIR / "positions.csv",
    "trade_log":    SCRIPTS_DIR / "trade_log.csv",
    "pipeline_log": SCRIPTS_DIR / "pipeline.log",
    "backtest":     SCRIPTS_DIR / "backtest_results.csv",
    "optimizer":    SCRIPTS_DIR / "optimizer_results.csv",
    "stage1":       DATASETS_DIR / "filtered_training_set.csv",
    "stage2":       DATASETS_DIR / "stage2_with_tickers.csv",
    "stage3":       DATASETS_DIR / "training_set_final.csv",
}

# ─── Dark Theme (Catppuccin Mocha) ────────────────────────────────────────────

DARK_QSS = """
QMainWindow, QWidget, QDialog {
    background-color: #1e1e2e;
    color: #cdd6f4;
}
QTabWidget::pane {
    border: 1px solid #45475a;
    background-color: #1e1e2e;
}
QTabBar::tab {
    background: #181825;
    color: #a6adc8;
    padding: 7px 18px;
    border: 1px solid #313244;
    border-bottom: none;
    border-top-left-radius: 4px;
    border-top-right-radius: 4px;
    min-width: 100px;
}
QTabBar::tab:selected {
    background: #313244;
    color: #89b4fa;
    border-color: #45475a;
}
QTabBar::tab:hover:!selected {
    background: #252535;
    color: #cdd6f4;
}
QPushButton {
    background: #313244;
    color: #cdd6f4;
    border: 1px solid #45475a;
    padding: 6px 14px;
    border-radius: 4px;
    font-size: 13px;
}
QPushButton:hover { background: #45475a; }
QPushButton:disabled { background: #181825; color: #585b70; border-color: #313244; }
QPushButton#start_btn { background: #40a02b; color: #e6e9ef; border-color: #40a02b; font-weight: bold; }
QPushButton#start_btn:hover { background: #4cb230; }
QPushButton#stop_btn { background: #d20f39; color: #e6e9ef; border-color: #d20f39; font-weight: bold; }
QPushButton#stop_btn:hover { background: #e81040; }
QPushButton#apply_btn { background: #89b4fa; color: #1e1e2e; border-color: #89b4fa; font-weight: bold; }
QPushButton#apply_btn:hover { background: #99c0fb; }
QPushButton#run_btn { background: #40a02b; color: #e6e9ef; border-color: #40a02b; font-weight: bold; }
QPushButton#run_btn:hover { background: #4cb230; }
QTableView {
    background-color: #181825;
    alternate-background-color: #1e1e2e;
    gridline-color: #313244;
    selection-background-color: #313244;
    selection-color: #cdd6f4;
    border: 1px solid #45475a;
}
QTableView::item:selected { background-color: #45475a; }
QHeaderView::section {
    background-color: #313244;
    color: #89b4fa;
    border: none;
    border-right: 1px solid #45475a;
    border-bottom: 1px solid #45475a;
    padding: 5px 8px;
    font-weight: bold;
}
QHeaderView::section:hover { background-color: #45475a; }
QPlainTextEdit {
    background-color: #11111b;
    color: #a6e3a1;
    font-family: Consolas, "Courier New", monospace;
    font-size: 12px;
    border: 1px solid #45475a;
}
QLineEdit, QDateEdit, QDoubleSpinBox, QSpinBox, QComboBox {
    background-color: #313244;
    color: #cdd6f4;
    border: 1px solid #45475a;
    padding: 4px 8px;
    border-radius: 3px;
    min-height: 22px;
}
QLineEdit:focus, QDateEdit:focus, QDoubleSpinBox:focus, QSpinBox:focus, QComboBox:focus {
    border-color: #89b4fa;
}
QComboBox::drop-down { border: none; width: 20px; }
QComboBox QAbstractItemView {
    background-color: #313244;
    color: #cdd6f4;
    selection-background-color: #45475a;
    border: 1px solid #45475a;
}
QSpinBox::up-button, QSpinBox::down-button,
QDoubleSpinBox::up-button, QDoubleSpinBox::down-button {
    background-color: #45475a;
    border: none;
    width: 16px;
}
QSpinBox::up-button:hover, QDoubleSpinBox::up-button:hover,
QSpinBox::down-button:hover, QDoubleSpinBox::down-button:hover {
    background-color: #585b70;
}
QGroupBox {
    border: 1px solid #45475a;
    border-radius: 6px;
    margin-top: 10px;
    padding-top: 6px;
    font-size: 13px;
}
QGroupBox::title {
    subcontrol-origin: margin;
    subcontrol-position: top left;
    padding: 0 6px;
    color: #89b4fa;
    font-weight: bold;
}
QScrollBar:vertical {
    background: #181825;
    width: 10px;
    border: none;
}
QScrollBar::handle:vertical {
    background: #45475a;
    border-radius: 5px;
    min-height: 20px;
}
QScrollBar::handle:vertical:hover { background: #585b70; }
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
QScrollBar:horizontal {
    background: #181825;
    height: 10px;
    border: none;
}
QScrollBar::handle:horizontal {
    background: #45475a;
    border-radius: 5px;
    min-width: 20px;
}
QScrollBar::handle:horizontal:hover { background: #585b70; }
QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal { width: 0; }
QProgressBar {
    background-color: #313244;
    border: 1px solid #45475a;
    border-radius: 3px;
    text-align: center;
    color: #cdd6f4;
}
QProgressBar::chunk { background-color: #89b4fa; border-radius: 3px; }
QCheckBox { color: #cdd6f4; spacing: 6px; }
QCheckBox::indicator {
    width: 16px; height: 16px;
    background: #313244;
    border: 1px solid #45475a;
    border-radius: 3px;
}
QCheckBox::indicator:checked { background: #89b4fa; border-color: #89b4fa; }
QSplitter::handle { background: #45475a; }
QSplitter::handle:horizontal { width: 3px; }
QSplitter::handle:vertical { height: 3px; }
QStatusBar { background: #181825; color: #a6adc8; border-top: 1px solid #45475a; }
QScrollArea { border: none; }
QLabel#status_running { color: #a6e3a1; font-weight: bold; }
QLabel#status_stopped { color: #f38ba8; font-weight: bold; }
QLabel#status_ok { color: #a6e3a1; }
QLabel#status_missing { color: #f38ba8; }
QLabel#metric_value { color: #89b4fa; font-size: 18px; font-weight: bold; }
QLabel#section_header { color: #89b4fa; font-size: 14px; font-weight: bold; }
"""

# ─── CSVTableModel ─────────────────────────────────────────────────────────────

class CSVTableModel(QAbstractTableModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._headers: list[str] = []
        self._rows: list[dict] = []

    def load(self, path: Path, max_rows: int = 0, newest_first: bool = False):
        self.beginResetModel()
        self._headers, self._rows = _load_csv(path)
        if newest_first:
            self._rows = list(reversed(self._rows))
        if max_rows > 0:
            self._rows = self._rows[:max_rows]
        self.endResetModel()

    def rowCount(self, parent=QModelIndex()):
        return len(self._rows)

    def columnCount(self, parent=QModelIndex()):
        return len(self._headers)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
        if role == Qt.ItemDataRole.DisplayRole:
            col = self._headers[index.column()]
            return self._rows[index.row()].get(col, "")
        if role == Qt.ItemDataRole.TextAlignmentRole:
            col = self._headers[index.column()]
            val = self._rows[index.row()].get(col, "")
            try:
                float(val)
                return Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
            except (ValueError, TypeError):
                return Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
        if role == Qt.ItemDataRole.ForegroundRole:
            col = self._headers[index.column()]
            val = self._rows[index.row()].get(col, "")
            if col in ("pnl_pct", "total_pnl_pct", "expectancy"):
                try:
                    v = float(val)
                    return QColor("#a6e3a1") if v >= 0 else QColor("#f38ba8")
                except (ValueError, TypeError):
                    pass
        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role == Qt.ItemDataRole.DisplayRole:
            if orientation == Qt.Orientation.Horizontal and section < len(self._headers):
                return self._headers[section]
        return None

    def get_column_values(self, col: str) -> list[float]:
        out = []
        for row in self._rows:
            try:
                out.append(float(row.get(col, "")))
            except (ValueError, TypeError):
                pass
        return out


class NumericSortProxyModel(QSortFilterProxyModel):
    def lessThan(self, left, right):
        l_data = left.data(Qt.ItemDataRole.DisplayRole) or ""
        r_data = right.data(Qt.ItemDataRole.DisplayRole) or ""
        try:
            return float(l_data) < float(r_data)
        except (ValueError, TypeError):
            return str(l_data).lower() < str(r_data).lower()


def _load_csv(path: Path) -> tuple[list[str], list[dict]]:
    if not path.exists():
        return [], []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            headers = list(reader.fieldnames or [])
        return headers, rows
    except Exception:
        return [], []


def _file_stat(path: Path) -> str:
    if not path.exists():
        return "Not found"
    size = path.stat().st_size
    if size > 1_000_000:
        size_str = f"{size/1_000_000:.1f} MB"
    elif size > 1_000:
        size_str = f"{size/1_000:.1f} KB"
    else:
        size_str = f"{size} B"
    _, rows = _load_csv(path)
    row_str = f"{len(rows):,} rows" if rows else "0 rows"
    import datetime
    mtime = datetime.datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
    return f"{row_str} · {size_str} · {mtime}"


# ─── LogViewer ────────────────────────────────────────────────────────────────

class LogViewer(QPlainTextEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setReadOnly(True)
        self.setMaximumBlockCount(2000)
        font = QFont("Consolas", 11)
        font.setStyleHint(QFont.StyleHint.Monospace)
        self.setFont(font)

    def append(self, text: str):
        self.appendPlainText(text.rstrip())
        sb = self.verticalScrollBar()
        sb.setValue(sb.maximum())


# ─── MPLCanvas ────────────────────────────────────────────────────────────────

class MPLCanvas(FigureCanvasQTAgg):
    def __init__(self, parent=None):
        self.fig = Figure(facecolor="#1e1e2e", tight_layout=True)
        super().__init__(self.fig)
        self.setParent(parent)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

    def plot_cumulative_pnl(self, values: list[float], title: str = "Cumulative P&L"):
        self.fig.clear()
        ax = self.fig.add_subplot(111)
        ax.set_facecolor("#11111b")
        ax.tick_params(colors="#cdd6f4")
        ax.xaxis.label.set_color("#cdd6f4")
        ax.yaxis.label.set_color("#cdd6f4")
        ax.title.set_color("#89b4fa")
        for spine in ax.spines.values():
            spine.set_edgecolor("#45475a")
        ax.grid(color="#313244", linestyle="--", linewidth=0.5)

        if values:
            cumulative = []
            running = 0.0
            for v in values:
                running += v
                cumulative.append(running)
            color = "#a6e3a1" if cumulative[-1] >= 0 else "#f38ba8"
            ax.plot(cumulative, color=color, linewidth=1.5)
            ax.axhline(0, color="#585b70", linewidth=0.8, linestyle="--")
            ax.set_xlabel("Trade #", color="#cdd6f4")
            ax.set_ylabel("Cumulative P&L %", color="#cdd6f4")
        ax.set_title(title, color="#89b4fa")
        self.draw()

    def clear_plot(self):
        self.fig.clear()
        ax = self.fig.add_subplot(111)
        ax.set_facecolor("#11111b")
        for spine in ax.spines.values():
            spine.set_edgecolor("#45475a")
        ax.set_title("No data", color="#585b70")
        self.draw()


# ─── ProcessManager ───────────────────────────────────────────────────────────

class ProcessManager:
    """Central registry for all QProcess instances."""

    def __init__(self):
        self._procs: dict[str, QProcess] = {}
        self._log_widgets: dict[str, LogViewer] = {}
        self._finished_callbacks: dict[str, list] = {}

    def start(self, name: str, args: list[str], log_widget: LogViewer | None = None,
              on_finished=None):
        if self.is_running(name):
            return
        proc = QProcess()
        proc.setProcessChannelMode(QProcess.ProcessChannelMode.MergedChannels)
        env = QProcessEnvironment.systemEnvironment()
        proc.setProcessEnvironment(env)

        # Suppress console flash on Windows
        if sys.platform == "win32":
            try:
                proc.setCreateProcessArgumentsModifier(
                    lambda a: setattr(a, "creationFlags", a.creationFlags | 0x08000000)
                )
            except Exception:
                pass

        if log_widget:
            proc.readyReadStandardOutput.connect(
                lambda p=proc, lw=log_widget: lw.append(
                    bytes(p.readAllStandardOutput()).decode("utf-8", errors="replace")
                )
            )

        cbs = []
        if on_finished:
            cbs.append(on_finished)
        self._finished_callbacks[name] = cbs

        proc.finished.connect(lambda code, status, n=name: self._on_finished(n, code))
        self._procs[name] = proc
        if log_widget:
            self._log_widgets[name] = log_widget

        if log_widget:
            log_widget.append(f"▶ {Path(args[1]).name if len(args) > 1 else ''} {' '.join(args[2:])}\n")

        proc.start(sys.executable, args)

    def stop(self, name: str):
        proc = self._procs.get(name)
        if proc and proc.state() != QProcess.ProcessState.NotRunning:
            proc.kill()

    def is_running(self, name: str) -> bool:
        proc = self._procs.get(name)
        return proc is not None and proc.state() != QProcess.ProcessState.NotRunning

    def any_running(self, *names: str) -> bool:
        return any(self.is_running(n) for n in names)

    def _on_finished(self, name: str, code: int):
        lw = self._log_widgets.get(name)
        if lw:
            lw.append(f"\n✓ Process finished (exit code {code})\n")
        for cb in self._finished_callbacks.get(name, []):
            try:
                cb(code)
            except Exception:
                pass


PROC = ProcessManager()


# ─── CSVWatcher ───────────────────────────────────────────────────────────────

class CSVWatcher:
    def __init__(self):
        self._watcher = QFileSystemWatcher()
        self._callbacks: dict[str, list] = {}
        self._watcher.fileChanged.connect(self._dispatch)

    def watch(self, path: Path, callback):
        p = str(path)
        if path.exists():
            self._watcher.addPath(p)
        self._callbacks.setdefault(p, []).append(callback)

    def _dispatch(self, changed_path: str):
        # Re-add: Windows drops watch after atomic file replace
        if changed_path not in self._watcher.files():
            if Path(changed_path).exists():
                self._watcher.addPath(changed_path)
        for cb in self._callbacks.get(changed_path, []):
            try:
                cb()
            except Exception:
                pass

    def refresh_watch(self, path: Path):
        """Call after a file is created to start watching it."""
        p = str(path)
        if path.exists() and p not in self._watcher.files():
            self._watcher.addPath(p)


WATCHER = CSVWatcher()


# ─── Shared table builder ─────────────────────────────────────────────────────

def make_table(show_cols: list[str] | None = None) -> tuple[QTableView, CSVTableModel, NumericSortProxyModel]:
    model = CSVTableModel()
    proxy = NumericSortProxyModel()
    proxy.setSourceModel(model)
    view = QTableView()
    view.setModel(proxy)
    view.setSortingEnabled(True)
    view.setAlternatingRowColors(True)
    view.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
    # Interactive is much faster than ResizeToContents (no per-cell measurement)
    hh = view.horizontalHeader()
    hh.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
    hh.setDefaultSectionSize(110)
    hh.setStretchLastSection(True)
    view.verticalHeader().setVisible(False)
    view.verticalHeader().setDefaultSectionSize(22)
    view.setHorizontalScrollMode(QTableView.ScrollMode.ScrollPerPixel)
    view.setVerticalScrollMode(QTableView.ScrollMode.ScrollPerPixel)
    return view, model, proxy


def _summary_stats(rows: list[dict]) -> dict:
    """Compute comprehensive summary stats from backtest result rows."""
    if not rows:
        return {}

    pnls = []
    peak_pnls = []
    return_7ds = []
    tp_hits = sl_hits = timeouts = 0

    for r in rows:
        try:
            pnl = float(r.get("pnl_pct", 0))
            pnls.append(pnl)
        except (ValueError, TypeError):
            pass
        try:
            peak = float(r.get("peak_pnl_pct", 0))
            peak_pnls.append(peak)
        except (ValueError, TypeError):
            pass
        try:
            ret_7d = float(r.get("return_t7", 0))
            if ret_7d != 0:  # Only include if populated
                return_7ds.append(ret_7d)
        except (ValueError, TypeError):
            pass

        if str(r.get("hit_tp", "")).lower() in ("true", "1", "yes"):
            tp_hits += 1
        elif str(r.get("hit_sl", "")).lower() in ("true", "1", "yes"):
            sl_hits += 1
        else:
            timeouts += 1

    if not pnls:
        return {}

    n = len(pnls)
    wins = sum(1 for p in pnls if p > 0)
    losses = n - wins

    # Basic stats
    total_pnl = sum(pnls)
    avg_pnl = total_pnl / n
    win_rate = wins / n * 100 if n > 0 else 0

    # Best/worst trades
    best_trade = max(pnls)
    worst_trade = min(pnls)

    # Win/loss stats
    winning_trades = [p for p in pnls if p > 0]
    losing_trades = [p for p in pnls if p < 0]
    avg_win = sum(winning_trades) / len(winning_trades) if winning_trades else 0
    avg_loss = sum(losing_trades) / len(losing_trades) if losing_trades else 0

    # Profit factor
    sum_wins = sum(winning_trades)
    sum_losses = abs(sum(losing_trades))
    profit_factor = sum_wins / sum_losses if sum_losses > 0 else 0

    # Expectancy
    expectancy = (win_rate / 100 * avg_win) - ((1 - win_rate / 100) * abs(avg_loss))

    # Sharpe ratio (assuming 252 trading days per year)
    if len(pnls) > 1:
        import statistics
        std_dev = statistics.stdev(pnls)
        sharpe = (avg_pnl / std_dev * (252 ** 0.5)) if std_dev > 0 else 0
    else:
        sharpe = 0

    # Max drawdown (cumulative approach)
    cumulative = 0
    peak = 0
    max_dd = 0
    for p in pnls:
        cumulative += p
        if cumulative > peak:
            peak = cumulative
        drawdown = peak - cumulative
        if drawdown > max_dd:
            max_dd = drawdown

    # Peak intraday and 7-day returns
    avg_peak_intraday = sum(peak_pnls) / len(peak_pnls) if peak_pnls else 0
    avg_7day_return = sum(return_7ds) / len(return_7ds) if return_7ds else 0
    peak_single_trade = max(peak_pnls) if peak_pnls else 0

    return {
        "trades":              n,
        "win_rate":            win_rate,
        "total_pnl":           total_pnl,
        "avg_pnl":             avg_pnl,
        "best":                best_trade,
        "worst":               worst_trade,
        "tp_pct":              tp_hits / n * 100,
        "sl_pct":              sl_hits / n * 100,
        "timeout_pct":         timeouts / n * 100,
        "avg_win":             avg_win,
        "avg_loss":            avg_loss,
        "profit_factor":       profit_factor,
        "expectancy":          expectancy,
        "sharpe_ratio":        sharpe,
        "max_drawdown":        max_dd,
        "avg_peak_intraday":   avg_peak_intraday,
        "avg_7day_return":     avg_7day_return,
        "peak_single_trade":   peak_single_trade,
    }


# ─── DashboardTab ─────────────────────────────────────────────────────────────

class DashboardTab(QWidget):
    def __init__(self, pipeline_status_label: QLabel, parent=None):
        super().__init__(parent)
        self._status_label = pipeline_status_label
        self._build_ui()
        self._setup_watchers()
        QTimer.singleShot(100, self._refresh_all)

    def _build_ui(self):
        root = QHBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(12)

        # ── Left column: status cards ──
        left = QVBoxLayout()
        left.setSpacing(10)

        status_box = QGroupBox("Pipeline Status")
        sf = QFormLayout(status_box)
        self._lbl_state = QLabel("Stopped")
        self._lbl_state.setObjectName("status_stopped")
        self._lbl_last_run = QLabel("—")
        self._lbl_positions = QLabel("—")
        self._lbl_signals_today = QLabel("—")
        sf.addRow("State:", self._lbl_state)
        sf.addRow("Last log update:", self._lbl_last_run)
        sf.addRow("Open positions:", self._lbl_positions)
        sf.addRow("Total signals:", self._lbl_signals_today)
        left.addWidget(status_box)

        refresh_btn = QPushButton("⟳  Refresh")
        refresh_btn.clicked.connect(self._refresh_all)
        left.addWidget(refresh_btn)
        left.addStretch()

        root.addLayout(left, 1)

        # ── Right column: mini tables ──
        right = QVBoxLayout()
        right.setSpacing(10)

        pos_box = QGroupBox("Open Positions")
        pbl = QVBoxLayout(pos_box)
        self._pos_view, self._pos_model, _ = make_table()
        pbl.addWidget(self._pos_view)
        right.addWidget(pos_box, 1)

        sig_box = QGroupBox("Recent Signals (last 20)")
        sbl = QVBoxLayout(sig_box)
        self._sig_view, self._sig_model, _ = make_table()
        sbl.addWidget(self._sig_view)
        right.addWidget(sig_box, 2)

        root.addLayout(right, 3)

    def _setup_watchers(self):
        WATCHER.watch(OUTPUT_FILES["positions"], self._reload_positions)
        WATCHER.watch(OUTPUT_FILES["signal_log"], self._reload_signals)
        WATCHER.watch(OUTPUT_FILES["pipeline_log"], self._reload_status)

    def _refresh_all(self):
        self._reload_positions()
        self._reload_signals()
        self._reload_status()

    def _reload_positions(self):
        self._pos_model.load(OUTPUT_FILES["positions"])
        count = self._pos_model.rowCount()
        self._lbl_positions.setText(str(count) if count else "0")
        WATCHER.refresh_watch(OUTPUT_FILES["positions"])

    def _reload_signals(self):
        self._sig_model.load(OUTPUT_FILES["signal_log"], max_rows=20, newest_first=True)
        count = self._sig_model.rowCount()
        self._lbl_signals_today.setText(str(count) if count else "0")
        WATCHER.refresh_watch(OUTPUT_FILES["signal_log"])

    def _reload_status(self):
        log_path = OUTPUT_FILES["pipeline_log"]
        if log_path.exists():
            import datetime
            mtime = datetime.datetime.fromtimestamp(log_path.stat().st_mtime)
            self._lbl_last_run.setText(mtime.strftime("%Y-%m-%d %H:%M:%S"))
        WATCHER.refresh_watch(OUTPUT_FILES["pipeline_log"])

    def update_pipeline_state(self, running: bool):
        if running:
            self._lbl_state.setText("Running")
            self._lbl_state.setObjectName("status_running")
        else:
            self._lbl_state.setText("Stopped")
            self._lbl_state.setObjectName("status_stopped")
        self._lbl_state.setStyleSheet(
            "color: #a6e3a1; font-weight: bold;" if running else "color: #f38ba8; font-weight: bold;"
        )


# ─── LivePipelineTab ──────────────────────────────────────────────────────────

class LivePipelineTab(QWidget):
    pipeline_state_changed = pyqtSignal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._log_tail_size = 0
        self._build_ui()
        self._setup_log_timer()
        self._setup_watchers()
        QTimer.singleShot(150, self._reload_tables)

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)

        # Control bar
        ctrl = QHBoxLayout()
        self._btn_start = QPushButton("▶  Start Pipeline")
        self._btn_start.setObjectName("start_btn")
        self._btn_start.clicked.connect(self._start_pipeline)
        self._btn_stop = QPushButton("■  Force Stop")
        self._btn_stop.setObjectName("stop_btn")
        self._btn_stop.setEnabled(False)
        self._btn_stop.clicked.connect(self._stop_pipeline)
        self._btn_clear = QPushButton("Clear Log")
        self._btn_clear.clicked.connect(lambda: self._log.clear())
        self._lbl_state = QLabel("Stopped")
        self._lbl_state.setStyleSheet("color: #f38ba8; font-weight: bold; font-size: 13px;")
        ctrl.addWidget(self._btn_start)
        ctrl.addWidget(self._btn_stop)
        ctrl.addSpacing(12)
        ctrl.addWidget(QLabel("Status:"))
        ctrl.addWidget(self._lbl_state)
        ctrl.addStretch()
        ctrl.addWidget(self._btn_clear)
        root.addLayout(ctrl)

        # Splitter: log top, tables bottom
        splitter = QSplitter(Qt.Orientation.Vertical)

        self._log = LogViewer()
        self._log.setPlaceholderText("Pipeline output will appear here...")
        splitter.addWidget(self._log)

        bottom = QSplitter(Qt.Orientation.Horizontal)

        pos_box = QGroupBox("Open Positions")
        pbl = QVBoxLayout(pos_box)
        self._pos_view, self._pos_model, _ = make_table()
        pbl.addWidget(self._pos_view)
        bottom.addWidget(pos_box)

        sig_box = QGroupBox("Signal Log (last 100)")
        sbl = QVBoxLayout(sig_box)
        self._sig_view, self._sig_model, _ = make_table()
        sbl.addWidget(self._sig_view)
        bottom.addWidget(sig_box)

        bottom.setSizes([400, 600])
        splitter.addWidget(bottom)
        splitter.setSizes([400, 300])
        root.addWidget(splitter)

    def _setup_log_timer(self):
        self._log_timer = QTimer()
        self._log_timer.setInterval(2000)
        self._log_timer.timeout.connect(self._tail_log_file)
        self._log_timer.start()

    def _setup_watchers(self):
        WATCHER.watch(OUTPUT_FILES["positions"], self._reload_tables)
        WATCHER.watch(OUTPUT_FILES["signal_log"], self._reload_tables)

    def _tail_log_file(self):
        log_path = OUTPUT_FILES["pipeline_log"]
        if not log_path.exists():
            return
        size = log_path.stat().st_size
        if size <= self._log_tail_size:
            return
        try:
            with open(log_path, "rb") as f:
                f.seek(self._log_tail_size)
                new_bytes = f.read()
            self._log.append(new_bytes.decode("utf-8", errors="replace"))
            self._log_tail_size = size
        except Exception:
            pass

    def _reload_tables(self):
        self._pos_model.load(OUTPUT_FILES["positions"])
        self._sig_model.load(OUTPUT_FILES["signal_log"], max_rows=100, newest_first=True)
        WATCHER.refresh_watch(OUTPUT_FILES["positions"])
        WATCHER.refresh_watch(OUTPUT_FILES["signal_log"])

    def _start_pipeline(self):
        self._log.clear()
        self._log_tail_size = 0
        PROC.start(
            "pipeline",
            [str(SCRIPTS_DIR / "main.py")],
            log_widget=self._log,
            on_finished=self._on_pipeline_finished,
        )
        self._set_running(True)

    def _stop_pipeline(self):
        PROC.stop("pipeline")

    def _on_pipeline_finished(self, code: int):
        self._set_running(False)

    def _set_running(self, running: bool):
        self._btn_start.setEnabled(not running)
        self._btn_stop.setEnabled(running)
        if running:
            self._lbl_state.setText("Running")
            self._lbl_state.setStyleSheet("color: #a6e3a1; font-weight: bold; font-size: 13px;")
        else:
            self._lbl_state.setText("Stopped")
            self._lbl_state.setStyleSheet("color: #f38ba8; font-weight: bold; font-size: 13px;")
        self.pipeline_state_changed.emit(running)


# ─── Stat card helper ────────────────────────────────────────────────────────

def _make_stat_card(label: str, value: str, value_color: str = "#cdd6f4") -> QWidget:
    card = QWidget()
    card.setStyleSheet(
        "QWidget { background: #313244; border: 1px solid #45475a; border-radius: 6px; }"
    )
    lay = QVBoxLayout(card)
    lay.setContentsMargins(10, 8, 10, 8)
    lay.setSpacing(2)
    val_lbl = QLabel(value)
    val_lbl.setStyleSheet(f"color: {value_color}; font-size: 20px; font-weight: bold; border: none;")
    val_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
    lbl_lbl = QLabel(label)
    lbl_lbl.setStyleSheet("color: #a6adc8; font-size: 11px; border: none;")
    lbl_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
    lay.addWidget(val_lbl)
    lay.addWidget(lbl_lbl)
    return card


# ─── TradesDialog ─────────────────────────────────────────────────────────────

class TradesDialog(QWidget):
    """Floating window showing individual trade rows."""
    def __init__(self, path: Path, title: str, parent=None):
        super().__init__(parent, Qt.WindowType.Window)
        self.setWindowTitle(title)
        self.resize(1100, 600)
        lay = QVBoxLayout(self)
        lay.setContentsMargins(8, 8, 8, 8)
        view, model, _ = make_table()
        model.load(path, newest_first=True)
        lay.addWidget(view)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.close)
        lay.addWidget(close_btn)
        self._model = model


# ─── BacktestTab ──────────────────────────────────────────────────────────────

class BacktestTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._trades_window = None
        self._build_ui()
        self._populate_years()
        # Defer load so startup isn't blocked
        QTimer.singleShot(200, self._load_results)

    def apply_optimizer_params(self, tp: float, sl: float, hold: int, threshold: int):
        """Called from OptimizerTab to push best params here."""
        self._mode_combo.setCurrentIndex(1)   # switch to "Optimized"
        self._tp.setValue(tp)
        self._sl.setValue(sl)
        self._hold.setValue(hold)
        self._threshold.setValue(threshold)
        self._on_mode_change(1)

    def _build_ui(self):
        root = QHBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        # ── Left panel ──
        left = QWidget()
        left.setFixedWidth(250)
        lv = QVBoxLayout(left)
        lv.setContentsMargins(0, 0, 0, 0)
        lv.setSpacing(10)

        # Mode selector
        mode_box = QGroupBox("Parameter Mode")
        mbl = QVBoxLayout(mode_box)
        self._mode_combo = QComboBox()
        self._mode_combo.addItems(["Custom", "Optimized (best from optimizer)"])
        self._mode_combo.currentIndexChanged.connect(self._on_mode_change)
        mbl.addWidget(self._mode_combo)
        lv.addWidget(mode_box)

        # Year selection
        year_box = QGroupBox("Data Year")
        ybf = QFormLayout(year_box)
        ybf.setSpacing(6)
        self._year_combo = QComboBox()
        self._year_combo.currentIndexChanged.connect(self._on_year_changed)
        ybf.addRow("Year:", self._year_combo)
        lv.addWidget(year_box)

        # Parameters
        self._params_box = QGroupBox("Parameters")
        pf = QFormLayout(self._params_box)
        pf.setSpacing(6)

        self._tp = QDoubleSpinBox()
        self._tp.setRange(0.01, 0.50); self._tp.setSingleStep(0.01); self._tp.setValue(0.08)
        self._tp.setDecimals(2); self._tp.setSuffix("  (8%)")
        self._tp.valueChanged.connect(lambda v: self._tp.setSuffix(f"  ({v*100:.0f}%)"))

        self._sl = QDoubleSpinBox()
        self._sl.setRange(0.01, 0.50); self._sl.setSingleStep(0.01); self._sl.setValue(0.07)
        self._sl.setDecimals(2); self._sl.setSuffix("  (7%)")
        self._sl.valueChanged.connect(lambda v: self._sl.setSuffix(f"  ({v*100:.0f}%)"))

        self._hold = QSpinBox()
        self._hold.setRange(1, 30); self._hold.setValue(4); self._hold.setSuffix(" days")

        self._threshold = QSpinBox()
        self._threshold.setRange(0, 100); self._threshold.setValue(40)

        pf.addRow("Take profit:", self._tp)
        pf.addRow("Stop loss:",   self._sl)
        pf.addRow("Hold days:",   self._hold)
        pf.addRow("Threshold:",   self._threshold)
        lv.addWidget(self._params_box)

        # Dataset label (always uses training_set_final.csv)
        ds_lbl = QLabel(f"Dataset: training_set_final.csv")
        ds_lbl.setStyleSheet("color: #a6adc8; font-size: 11px;")
        ds_lbl.setWordWrap(True)
        lv.addWidget(ds_lbl)

        self._run_btn = QPushButton("▶  Run Backtest")
        self._run_btn.setObjectName("run_btn")
        self._run_btn.clicked.connect(self._run_backtest)
        lv.addWidget(self._run_btn)

        self._progress = QProgressBar()
        self._progress.setRange(0, 0)
        self._progress.setVisible(False)
        lv.addWidget(self._progress)

        self._log = LogViewer()
        self._log.setMaximumHeight(120)
        self._log.setPlaceholderText("Output...")
        lv.addWidget(self._log)
        lv.addStretch()
        root.addWidget(left)

        # ── Right panel ──
        right = QVBoxLayout()
        right.setSpacing(8)

        # Summary cards row
        self._summary_box = QGroupBox("Results Summary")
        self._summary_lay = QVBoxLayout(self._summary_box)
        self._summary_placeholder = QLabel("Run a backtest to see results.")
        self._summary_placeholder.setStyleSheet("color: #585b70; font-size: 13px;")
        self._summary_placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._summary_lay.addWidget(self._summary_placeholder)
        right.addWidget(self._summary_box)

        # P&L chart
        chart_box = QGroupBox("Cumulative P&L")
        cbl = QVBoxLayout(chart_box)
        self._canvas = MPLCanvas()
        self._canvas.clear_plot()
        cbl.addWidget(self._canvas)
        right.addWidget(chart_box, 1)

        # View trades button
        self._trades_btn = QPushButton("📋  View Individual Trades")
        self._trades_btn.setEnabled(False)
        self._trades_btn.clicked.connect(self._show_trades)
        right.addWidget(self._trades_btn)

        container = QWidget()
        container.setLayout(right)
        root.addWidget(container)

    def _populate_years(self):
        """Scan for backtest_results_*.csv files and populate year dropdown."""
        import glob
        pattern = str(SCRIPTS_DIR / "backtest_results_*.csv")
        files = glob.glob(pattern)
        years = []
        for f in files:
            # Extract year from filename: backtest_results_2023.csv -> 2023
            fname = os.path.basename(f)
            try:
                year = fname.split("_")[2].split(".")[0]
                years.append(year)
            except (IndexError, ValueError):
                pass
        years.sort(reverse=True)  # Newest first
        self._year_combo.blockSignals(True)
        self._year_combo.clear()
        self._year_combo.addItems(years)
        self._year_combo.blockSignals(False)

    def _on_year_changed(self, idx: int):
        """Handle year dropdown selection change."""
        if idx >= 0:
            self._load_results()

    def _on_mode_change(self, idx: int):
        is_custom = (idx == 0)
        self._params_box.setEnabled(is_custom)
        if idx == 1:
            self._load_optimizer_params()

    def _load_optimizer_params(self):
        """Fill params from top row of optimizer_results.csv."""
        headers, rows = _load_csv(OUTPUT_FILES["optimizer"])
        if not rows:
            return
        # Sort by total_pnl_pct descending to get best row
        try:
            rows.sort(key=lambda r: float(r.get("total_pnl_pct", 0)), reverse=True)
        except Exception:
            pass
        best = rows[0]
        try: self._tp.setValue(float(best.get("tp_pct", 0.08)))
        except Exception: pass
        try: self._sl.setValue(float(best.get("sl_pct", 0.07)))
        except Exception: pass
        try: self._hold.setValue(int(float(best.get("max_hold_days", 4))))
        except Exception: pass
        try: self._threshold.setValue(int(float(best.get("score_threshold", 40))))
        except Exception: pass

    def _run_backtest(self):
        # Get selected year and generate date range
        year = self._year_combo.currentText()
        if not year:
            log.warning("No year selected for backtest")
            return
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"

        training_csv = str(OUTPUT_FILES["stage3"])
        args = [
            str(SCRIPTS_DIR / "backtest.py"),
            "--start",     start_date,
            "--end",       end_date,
            "--tp",        str(self._tp.value()),
            "--sl",        str(self._sl.value()),
            "--hold",      str(self._hold.value()),
            "--threshold", str(self._threshold.value()),
        ]
        if OUTPUT_FILES["stage3"].exists():
            args += ["--training-csv", training_csv]

        self._run_btn.setEnabled(False)
        self._progress.setVisible(True)
        self._log.clear()
        PROC.start("backtest", args, log_widget=self._log, on_finished=self._on_finished)

    def _on_finished(self, code: int):
        self._run_btn.setEnabled(True)
        self._progress.setVisible(False)
        self._load_results()

    def _load_results(self):
        # Get year-specific CSV file
        year = self._year_combo.currentText()
        if not year:
            path = OUTPUT_FILES["backtest"]
        else:
            path = SCRIPTS_DIR / f"backtest_results_{year}.csv"

        headers, rows = _load_csv(path)
        stats = _summary_stats(rows)

        # Update summary panel
        # Clear old widgets
        while self._summary_lay.count():
            item = self._summary_lay.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        if not stats:
            lbl = QLabel("No results yet. Run a backtest above.")
            lbl.setStyleSheet("color: #585b70; font-size: 13px;")
            lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self._summary_lay.addWidget(lbl)
            self._trades_btn.setEnabled(False)
        else:
            # Row 1: Main metrics
            row1 = QHBoxLayout()
            row1.setSpacing(6)
            pnl_color = "#a6e3a1" if stats["total_pnl"] >= 0 else "#f38ba8"
            wr_color  = "#a6e3a1" if stats["win_rate"] >= 50 else "#f38ba8"
            for label, value, color in [
                ("Trades",          str(stats["trades"]),                   "#cdd6f4"),
                ("Win Rate",        f"{stats['win_rate']:.1f}%",            wr_color),
                ("Total P&L",       f"{stats['total_pnl']:+.2f}%",          pnl_color),
                ("Avg/Trade",       f"{stats['avg_pnl']:+.2f}%",
                                     "#a6e3a1" if stats["avg_pnl"] >= 0 else "#f38ba8"),
            ]:
                card = _make_stat_card(label, value, color)
                row1.addWidget(card)
            self._summary_lay.addLayout(row1)

            # Row 2: Trade performance
            row2 = QHBoxLayout()
            row2.setSpacing(6)
            for label, value, color in [
                ("Best Trade",      f"{stats['best']:+.2f}%",               "#a6e3a1"),
                ("Worst Trade",     f"{stats['worst']:+.2f}%",              "#f38ba8"),
                ("Avg Win",         f"{stats['avg_win']:+.2f}%",            "#a6e3a1"),
                ("Avg Loss",        f"{stats['avg_loss']:+.2f}%",           "#f38ba8"),
            ]:
                card = _make_stat_card(label, value, color)
                row2.addWidget(card)
            self._summary_lay.addLayout(row2)

            # Row 3: Exit breakdown
            row3 = QHBoxLayout()
            row3.setSpacing(6)
            for label, value, color in [
                ("TP Exits",        f"{stats['tp_pct']:.0f}%",              "#89b4fa"),
                ("SL Exits",        f"{stats['sl_pct']:.0f}%",              "#f38ba8"),
                ("Timeouts",        f"{stats['timeout_pct']:.0f}%",         "#a6adc8"),
                ("Peak Intraday",   f"{stats['avg_peak_intraday']:+.2f}%",  "#fab387"),
            ]:
                card = _make_stat_card(label, value, color)
                row3.addWidget(card)
            self._summary_lay.addLayout(row3)

            # Row 4: Risk metrics
            row4 = QHBoxLayout()
            row4.setSpacing(6)
            for label, value, color in [
                ("Sharpe Ratio",    f"{stats['sharpe_ratio']:.2f}",         "#a6e3a1" if stats["sharpe_ratio"] > 1 else "#f38ba8"),
                ("Profit Factor",   f"{stats['profit_factor']:.2f}x",       "#a6e3a1" if stats["profit_factor"] > 1 else "#f38ba8"),
                ("Max Drawdown",    f"{stats['max_drawdown']:+.2f}%",       "#f38ba8"),
                ("Expectancy",      f"{stats['expectancy']:+.2f}%",         "#a6e3a1" if stats["expectancy"] >= 0 else "#f38ba8"),
            ]:
                card = _make_stat_card(label, value, color)
                row4.addWidget(card)
            self._summary_lay.addLayout(row4)

            # Row 5: Extended returns
            row5 = QHBoxLayout()
            row5.setSpacing(6)
            for label, value, color in [
                ("Peak Single Trade",   f"{stats['peak_single_trade']:+.2f}%", "#a6e3a1"),
                ("Avg 7-Day Return",    f"{stats['avg_7day_return']:+.2f}%",   "#89b4fa"),
            ]:
                card = _make_stat_card(label, value, color)
                row5.addWidget(card)
            row5.addStretch()
            self._summary_lay.addLayout(row5)

            self._trades_btn.setEnabled(True)

        # Update chart
        pnl_values = [float(r.get("pnl_pct", 0)) for r in rows
                      if r.get("pnl_pct") not in ("", None)]
        try:
            pnl_values = [float(v) for v in pnl_values]
        except Exception:
            pnl_values = []
        if pnl_values:
            self._canvas.plot_cumulative_pnl(pnl_values)
        else:
            self._canvas.clear_plot()
        WATCHER.refresh_watch(path)

    def _show_trades(self):
        if self._trades_window and not self._trades_window.isVisible():
            self._trades_window = None
        if not self._trades_window:
            year = self._year_combo.currentText()
            if year:
                results_file = SCRIPTS_DIR / f"backtest_results_{year}.csv"
            else:
                results_file = OUTPUT_FILES["backtest"]
            self._trades_window = TradesDialog(
                results_file, "Individual Trades — Backtest Results"
            )
        self._trades_window.show()
        self._trades_window.raise_()


# ─── OptimizerTab ─────────────────────────────────────────────────────────────

class OptimizerTab(QWidget):
    # Signal to push best params to the BacktestTab
    apply_params = pyqtSignal(float, float, int, int)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._results_window = None
        self._build_ui()
        QTimer.singleShot(300, self._load_results)

    def _build_ui(self):
        root = QHBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        # ── Left panel ──
        left = QWidget()
        left.setFixedWidth(250)
        lv = QVBoxLayout(left)
        lv.setContentsMargins(0, 0, 0, 0)
        lv.setSpacing(10)

        settings = QGroupBox("Optimizer Settings")
        sf = QFormLayout(settings)
        sf.setSpacing(8)

        self._mode = QComboBox()
        self._mode.addItems(["From Training CSV (offline)", "From Backtest Cache"])
        self._mode.currentIndexChanged.connect(self._on_mode_change)
        sf.addRow("Mode:", self._mode)

        self._csv_path = QLineEdit()
        self._csv_path.setPlaceholderText("Default: training_set_final.csv")
        self._csv_browse = QPushButton("Browse...")
        self._csv_browse.clicked.connect(self._browse_csv)
        csv_row = QHBoxLayout()
        csv_row.addWidget(self._csv_path)
        csv_row.addWidget(self._csv_browse)
        sf.addRow("Input CSV:", csv_row)

        one_year_ago = QDate.currentDate().addDays(-365)
        self._start_date = QDateEdit(one_year_ago)
        self._start_date.setCalendarPopup(True)
        self._start_date.setDisplayFormat("yyyy-MM-dd")
        self._end_date = QDateEdit(QDate.currentDate())
        self._end_date.setCalendarPopup(True)
        self._end_date.setDisplayFormat("yyyy-MM-dd")
        sf.addRow("Start date:", self._start_date)
        sf.addRow("End date:", self._end_date)
        lv.addWidget(settings)

        self._run_btn = QPushButton("▶  Run Optimizer")
        self._run_btn.setObjectName("run_btn")
        self._run_btn.clicked.connect(self._run_optimizer)
        lv.addWidget(self._run_btn)

        self._progress = QProgressBar()
        self._progress.setRange(0, 0)
        self._progress.setVisible(False)
        lv.addWidget(self._progress)

        warn = QLabel("⚠  Full grid: ~10–30 min")
        warn.setStyleSheet("color: #f9e2af; font-size: 11px;")
        lv.addWidget(warn)

        self._log = LogViewer()
        self._log.setMaximumHeight(140)
        self._log.setPlaceholderText("Output...")
        lv.addWidget(self._log)
        lv.addStretch()
        root.addWidget(left)

        # ── Right panel ──
        right = QVBoxLayout()
        right.setSpacing(8)

        # Best combo summary
        self._best_box = QGroupBox("Best Parameter Set")
        self._best_lay = QVBoxLayout(self._best_box)
        self._best_placeholder = QLabel("Run the optimizer to see results.")
        self._best_placeholder.setStyleSheet("color: #585b70; font-size: 13px;")
        self._best_placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._best_lay.addWidget(self._best_placeholder)
        right.addWidget(self._best_box)

        # Action buttons
        btn_row = QHBoxLayout()
        self._apply_btn = QPushButton("⚡  Apply Best Params to Backtest")
        self._apply_btn.setObjectName("apply_btn")
        self._apply_btn.setEnabled(False)
        self._apply_btn.clicked.connect(self._apply_best)
        self._view_all_btn = QPushButton("📋  View All Results")
        self._view_all_btn.setEnabled(False)
        self._view_all_btn.clicked.connect(self._show_all_results)
        btn_row.addWidget(self._apply_btn)
        btn_row.addWidget(self._view_all_btn)
        right.addLayout(btn_row)

        container = QWidget()
        container.setLayout(right)
        root.addWidget(container)

        self._best_row: dict = {}

    def _on_mode_change(self, idx: int):
        is_cache = idx == 1
        self._start_date.setEnabled(not is_cache)
        self._end_date.setEnabled(not is_cache)

    def _browse_csv(self):
        path, _ = QFileDialog.getOpenFileName(self, "Select CSV", str(DATASETS_DIR), "CSV (*.csv)")
        if path:
            self._csv_path.setText(path)

    def _run_optimizer(self):
        csv_path = self._csv_path.text() or str(OUTPUT_FILES["stage3"])
        if self._mode.currentIndex() == 0:
            args = [
                str(SCRIPTS_DIR / "optimizer.py"),
                "from-training-csv", csv_path,
                "--start", self._start_date.date().toString("yyyy-MM-dd"),
                "--end",   self._end_date.date().toString("yyyy-MM-dd"),
            ]
        else:
            cache = self._csv_path.text() or str(OUTPUT_FILES["backtest"])
            args = [str(SCRIPTS_DIR / "optimizer.py"), "from-cache", cache]

        self._run_btn.setEnabled(False)
        self._progress.setVisible(True)
        self._log.clear()
        PROC.start("optimizer", args, log_widget=self._log, on_finished=self._on_finished)

    def _on_finished(self, code: int):
        self._run_btn.setEnabled(True)
        self._progress.setVisible(False)
        self._load_results()

    def _load_results(self):
        headers, rows = _load_csv(OUTPUT_FILES["optimizer"])
        if not rows:
            WATCHER.refresh_watch(OUTPUT_FILES["optimizer"])
            return

        # Sort best first
        try:
            rows.sort(key=lambda r: float(r.get("total_pnl_pct", 0)), reverse=True)
        except Exception:
            pass
        self._best_row = rows[0] if rows else {}

        # Clear old summary widgets
        while self._best_lay.count():
            item = self._best_lay.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        best = self._best_row
        if not best:
            return

        # Row 1: param cards
        row1 = QHBoxLayout(); row1.setSpacing(6)
        def _pct(key, default=""):
            v = best.get(key, default)
            try: return f"{float(v)*100:.0f}%"
            except Exception: return str(v)
        def _val(key, default=""):
            return best.get(key, default)

        for label, value, color in [
            ("Threshold",  _val("score_threshold"), "#89b4fa"),
            ("Take Profit",_pct("tp_pct"),          "#a6e3a1"),
            ("Stop Loss",  _pct("sl_pct"),           "#f38ba8"),
            ("Hold Days",  _val("max_hold_days"),    "#cdf4f4"),
            ("Max Mkt Cap",f"${float(_val('max_mcap_M', _val('max_market_cap_M', 0)))/1000:.0f}B"
                            if _val("max_mcap_M") or _val("max_market_cap_M") else "—", "#f9e2af"),
        ]:
            row1.addWidget(_make_stat_card(label, value, color))
        self._best_lay.addLayout(row1)

        # Row 2: result cards
        row2 = QHBoxLayout(); row2.setSpacing(6)
        pnl = float(best.get("total_pnl_pct", 0))
        wr  = float(best.get("win_rate", 0))
        for label, value, color in [
            ("Total Return",  f"{pnl:+.2f}%",              "#a6e3a1" if pnl >= 0 else "#f38ba8"),
            ("Win Rate",      f"{float(best.get('win_rate', 0)):.1f}%",
                               "#a6e3a1" if wr >= 50 else "#f38ba8"),
            ("Trades",        _val("trades"),               "#cdd6f4"),
            ("Sharpe",        f"{float(best.get('sharpe', 0)):.2f}",  "#89b4fa"),
            ("Expectancy",    f"{float(best.get('expectancy', 0)):+.2f}%", "#cdd6f4"),
        ]:
            row2.addWidget(_make_stat_card(label, value, color))
        self._best_lay.addLayout(row2)

        self._apply_btn.setEnabled(True)
        self._view_all_btn.setEnabled(True)
        WATCHER.refresh_watch(OUTPUT_FILES["optimizer"])

    def _apply_best(self):
        b = self._best_row
        if not b:
            return
        try:
            tp  = float(b.get("tp_pct", 0.08))
            sl  = float(b.get("sl_pct", 0.07))
            hold = int(float(b.get("max_hold_days", 4)))
            thr  = int(float(b.get("score_threshold", 40)))
            self.apply_params.emit(tp, sl, hold, thr)
        except Exception:
            pass

    def _show_all_results(self):
        if self._results_window and not self._results_window.isVisible():
            self._results_window = None
        if not self._results_window:
            self._results_window = TradesDialog(
                OUTPUT_FILES["optimizer"], "All Optimizer Results"
            )
        self._results_window.show()
        self._results_window.raise_()


# ─── TrainingDataTab ──────────────────────────────────────────────────────────

class TrainingDataTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._build_ui()
        self._setup_watchers()
        self._refresh_status()

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        info = QLabel(
            "Download USASpending bulk CSV from "
            "<a href='https://files.usaspending.gov/award_data_archive/' style='color:#89b4fa'>"
            "files.usaspending.gov</a> and place in datasets/ before running."
        )
        info.setOpenExternalLinks(True)
        info.setWordWrap(True)
        info.setStyleSheet("color: #a6adc8; font-size: 12px;")
        root.addWidget(info)

        stages_row = QHBoxLayout()
        stages_row.setSpacing(10)

        # Stage 1
        s1 = QGroupBox("Stage 1 — Load & Filter")
        s1l = QVBoxLayout(s1)
        self._s1_status = QLabel("Checking...")
        self._s1_status.setObjectName("status_missing")
        self._s1_status.setWordWrap(True)
        s1l.addWidget(QLabel("Output: filtered_training_set.csv"))
        s1l.addWidget(self._s1_status)
        self._btn_s1 = QPushButton("▶  Run Build (all stages)")
        self._btn_s1.setObjectName("run_btn")
        self._btn_s1.clicked.connect(lambda: self._run_build("build"))
        s1l.addWidget(self._btn_s1)
        stages_row.addWidget(s1)

        # Stage 2
        s2 = QGroupBox("Stage 2 — Resolve Tickers")
        s2l = QVBoxLayout(s2)
        self._s2_status = QLabel("Checking...")
        self._s2_status.setObjectName("status_missing")
        self._s2_status.setWordWrap(True)
        s2l.addWidget(QLabel("Output: stage2_with_tickers.csv"))
        s2l.addWidget(self._s2_status)
        self._btn_s2 = QPushButton("⟳  Resume Build")
        self._btn_s2.clicked.connect(lambda: self._run_build("build"))
        s2l.addWidget(self._btn_s2)
        stages_row.addWidget(s2)

        # Stage 3
        s3 = QGroupBox("Stage 3 — Enrich OHLC")
        s3l = QVBoxLayout(s3)
        self._s3_status = QLabel("Checking...")
        self._s3_status.setObjectName("status_missing")
        self._s3_status.setWordWrap(True)
        s3l.addWidget(QLabel("Output: training_set_final.csv"))
        s3l.addWidget(self._s3_status)
        self._btn_s3 = QPushButton("▶  Enrich OHLC")
        self._btn_s3.setObjectName("run_btn")
        self._btn_s3.clicked.connect(self._run_enrich)
        s3l.addWidget(self._btn_s3)
        stages_row.addWidget(s3)

        root.addLayout(stages_row)

        self._progress = QProgressBar()
        self._progress.setRange(0, 0)
        self._progress.setVisible(False)
        root.addWidget(self._progress)

        log_box = QGroupBox("Output Log")
        logl = QVBoxLayout(log_box)
        self._log = LogViewer()
        logl.addWidget(self._log)
        root.addWidget(log_box)

    def _setup_watchers(self):
        for key in ("stage1", "stage2", "stage3"):
            WATCHER.watch(OUTPUT_FILES[key], self._refresh_status)

    def _refresh_status(self):
        for key, lbl in [("stage1", self._s1_status), ("stage2", self._s2_status), ("stage3", self._s3_status)]:
            path = OUTPUT_FILES[key]
            if path.exists():
                lbl.setText(_file_stat(path))
                lbl.setStyleSheet("color: #a6e3a1;")
            else:
                lbl.setText("Not found")
                lbl.setStyleSheet("color: #f38ba8;")
            WATCHER.refresh_watch(path)

    def _run_build(self, name: str):
        self._set_building(True)
        args = [str(SCRIPTS_DIR / "build_training_set.py"), "--quiet"]
        PROC.start(name, args, log_widget=self._log, on_finished=self._on_finished)

    def _run_enrich(self):
        stage3 = OUTPUT_FILES["stage3"]
        if not stage3.exists():
            QMessageBox.warning(self, "Missing file",
                                "training_set_final.csv not found. Run the build first.")
            return
        self._set_building(True)
        args = [str(SCRIPTS_DIR / "enrich_ohlc.py"), str(stage3)]
        PROC.start("enrich_ohlc", args, log_widget=self._log, on_finished=self._on_finished)

    def _on_finished(self, code: int):
        self._set_building(False)
        self._refresh_status()

    def _set_building(self, building: bool):
        self._progress.setVisible(building)
        for btn in (self._btn_s1, self._btn_s2, self._btn_s3):
            btn.setEnabled(not building)


# ─── ConfigTab ────────────────────────────────────────────────────────────────

class ConfigTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._widgets: dict[str, QWidget] = {}
        self._build_ui()
        self._load_config()

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        container = QWidget()
        form = QFormLayout(container)
        form.setSpacing(10)
        form.setContentsMargins(12, 12, 12, 12)

        def add_spinbox(key, label, min_v, max_v, suffix="", tooltip=""):
            sb = QSpinBox()
            sb.setRange(min_v, max_v)
            if suffix:
                sb.setSuffix(suffix)
            if tooltip:
                sb.setToolTip(tooltip)
            self._widgets[key] = sb
            form.addRow(label, sb)

        def add_double(key, label, min_v, max_v, step, suffix="", tooltip=""):
            sb = QDoubleSpinBox()
            sb.setRange(min_v, max_v)
            sb.setSingleStep(step)
            sb.setDecimals(3)
            if suffix:
                sb.setSuffix(suffix)
            if tooltip:
                sb.setToolTip(tooltip)
            self._widgets[key] = sb
            form.addRow(label, sb)

        # Filters section
        form.addRow(_section_label("Filter Thresholds"))
        add_spinbox("MAX_MARKET_CAP_M", "Max market cap", 1, 100_000, " M",
                    "Maximum company market cap to consider (millions)")
        add_spinbox("MIN_CONTRACT_VALUE_M", "Min contract value", 1, 10_000, " M",
                    "Minimum contract value (millions)")
        add_double("MAX_AWARD_AMOUNT_B", "Max award amount", 0.1, 100.0, 0.5, " B",
                   "Maximum award amount — skip mega-contracts (billions)")
        add_spinbox("TOP_N_TO_REMOVE", "Top N to remove (Stage 1)", 0, 100, "",
                    "Remove top-N companies by contract count in Stage 1")

        form.addRow(_section_label("Scoring"))
        add_spinbox("SCORE_THRESHOLD", "Score threshold", 0, 100, "",
                    "Minimum score (0-100) required to place a trade")

        form.addRow(_section_label("Trading Parameters"))
        add_double("TAKE_PROFIT_PCT", "Take profit", 0.01, 0.50, 0.01, "",
                   "Take profit as a decimal (e.g. 0.08 = 8%)")
        add_double("STOP_LOSS_PCT", "Stop loss", 0.01, 0.50, 0.01, "",
                   "Stop loss as a decimal (e.g. 0.07 = 7%)")
        add_spinbox("POSITION_SIZE", "Position size", 10, 100_000, " $",
                    "Dollar amount per trade")
        add_spinbox("MAX_HOLD_DAYS", "Max hold days", 1, 30, " days",
                    "Maximum trading days to hold a position")
        add_spinbox("POLL_INTERVAL_HOURS", "Poll interval", 1, 24, " hours",
                    "How often the live pipeline polls SAM.gov")

        form.addRow(_section_label("Backtest Filter Windows"))
        add_spinbox("MAX_8K_WINDOW_DAYS", "Max 8-K window", 0, 30, " days",
                    "Reject if 8-K filed within N days of award")
        add_spinbox("MAX_DILUTIVE_WINDOW_DAYS", "Max dilutive window", 0, 365, " days",
                    "Reject if S-1/S-3 filed within N days before award")
        add_spinbox("MAX_PR_WINDOW_DAYS", "Max PR window", 0, 30, " days",
                    "Reject if press release within N days of award")

        # Confidence combobox
        conf = QComboBox()
        conf.addItems(["none", "low", "medium", "medium_high", "high"])
        self._widgets["MIN_TICKER_CONFIDENCE"] = conf
        form.addRow("Min ticker confidence:", conf)

        scroll.setWidget(container)
        root.addWidget(scroll)

        # Buttons
        btn_row = QHBoxLayout()
        apply_btn = QPushButton("✓  Apply Changes")
        apply_btn.setObjectName("apply_btn")
        apply_btn.clicked.connect(self._apply_config)
        reload_btn = QPushButton("⟳  Reload from File")
        reload_btn.clicked.connect(self._load_config)
        btn_row.addStretch()
        btn_row.addWidget(reload_btn)
        btn_row.addWidget(apply_btn)
        root.addLayout(btn_row)

        self._status_lbl = QLabel("")
        self._status_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        root.addWidget(self._status_lbl)

    def _load_config(self):
        if not CONFIG_PATH.exists():
            return
        text = CONFIG_PATH.read_text(encoding="utf-8")

        def get_float(name):
            m = re.search(rf"^{name}\s*=\s*([0-9_\.eE+-]+)", text, re.MULTILINE)
            return float(m.group(1).replace("_", "")) if m else None

        def get_str(name):
            m = re.search(rf'^{name}\s*=\s*["\']([^"\']+)["\']', text, re.MULTILINE)
            return m.group(1) if m else None

        def set_spin(key, val):
            w = self._widgets.get(key)
            if w and val is not None:
                w.setValue(int(val))

        def set_double(key, val):
            w = self._widgets.get(key)
            if w and val is not None:
                w.setValue(float(val))

        v = get_float("MAX_MARKET_CAP")
        if v: set_spin("MAX_MARKET_CAP_M", v / 1_000_000)
        v = get_float("MIN_CONTRACT_VALUE")
        if v: set_spin("MIN_CONTRACT_VALUE_M", v / 1_000_000)
        v = get_float("MAX_AWARD_AMOUNT")
        if v: set_double("MAX_AWARD_AMOUNT_B", v / 1_000_000_000)

        for key in ("TOP_N_TO_REMOVE", "SCORE_THRESHOLD", "POSITION_SIZE",
                    "MAX_HOLD_DAYS", "POLL_INTERVAL_HOURS",
                    "MAX_8K_WINDOW_DAYS", "MAX_DILUTIVE_WINDOW_DAYS", "MAX_PR_WINDOW_DAYS"):
            v = get_float(key)
            if v is not None:
                set_spin(key, v)

        for key in ("TAKE_PROFIT_PCT", "STOP_LOSS_PCT"):
            v = get_float(key)
            if v is not None:
                set_double(key, v)

        conf_val = get_str("MIN_TICKER_CONFIDENCE")
        if conf_val:
            cb = self._widgets.get("MIN_TICKER_CONFIDENCE")
            idx = cb.findText(conf_val)
            if idx >= 0:
                cb.setCurrentIndex(idx)

        self._status_lbl.setText("")

    def _apply_config(self):
        if not CONFIG_PATH.exists():
            QMessageBox.critical(self, "Error", f"config.py not found at {CONFIG_PATH}")
            return
        text = CONFIG_PATH.read_text(encoding="utf-8")

        def repl(field, value_str):
            nonlocal text
            text = re.sub(
                rf"^({re.escape(field)}\s*=\s*).*$",
                rf"\g<1>{value_str}",
                text, flags=re.MULTILINE
            )

        mcap_m = self._widgets["MAX_MARKET_CAP_M"].value()
        repl("MAX_MARKET_CAP", f"{int(mcap_m * 1_000_000):_}")

        min_cv_m = self._widgets["MIN_CONTRACT_VALUE_M"].value()
        repl("MIN_CONTRACT_VALUE", f"{int(min_cv_m * 1_000_000):_}")

        max_aw_b = self._widgets["MAX_AWARD_AMOUNT_B"].value()
        repl("MAX_AWARD_AMOUNT", f"{int(max_aw_b * 1_000_000_000):_}")

        for key in ("TOP_N_TO_REMOVE", "SCORE_THRESHOLD", "POSITION_SIZE",
                    "MAX_HOLD_DAYS", "POLL_INTERVAL_HOURS",
                    "MAX_8K_WINDOW_DAYS", "MAX_DILUTIVE_WINDOW_DAYS", "MAX_PR_WINDOW_DAYS"):
            repl(key, str(self._widgets[key].value()))

        for key in ("TAKE_PROFIT_PCT", "STOP_LOSS_PCT"):
            repl(key, f"{self._widgets[key].value():.3f}")

        conf_val = self._widgets["MIN_TICKER_CONFIDENCE"].currentText()
        text = re.sub(
            r'^(MIN_TICKER_CONFIDENCE\s*=\s*).*$',
            rf'\1"{conf_val}"',
            text, flags=re.MULTILINE
        )

        CONFIG_PATH.write_text(text, encoding="utf-8")
        self._status_lbl.setText("✓ Saved. Restart the pipeline for changes to take effect.")
        self._status_lbl.setStyleSheet("color: #a6e3a1;")


def _section_label(text: str) -> QLabel:
    lbl = QLabel(text)
    lbl.setStyleSheet(
        "color: #89b4fa; font-weight: bold; font-size: 13px; "
        "padding-top: 8px; border-top: 1px solid #45475a;"
    )
    return lbl


# ─── MainWindow ───────────────────────────────────────────────────────────────

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("SAMgovArby Dashboard")
        self.resize(1280, 820)
        self._build_ui()

    def _build_ui(self):
        tabs = QTabWidget()
        tabs.setDocumentMode(True)

        # Status bar
        self._status_bar = QStatusBar()
        self.setStatusBar(self._status_bar)
        self._pipeline_status = QLabel("Pipeline: Stopped")
        self._pipeline_status.setStyleSheet("color: #f38ba8; font-weight: bold; padding: 0 8px;")
        self._status_bar.addPermanentWidget(self._pipeline_status)
        self._status_bar.showMessage("SAMgovArby ready")

        # Tabs
        self._dash_tab  = DashboardTab(self._pipeline_status)
        self._live_tab  = LivePipelineTab()
        self._live_tab.pipeline_state_changed.connect(self._on_pipeline_state)
        self._bt_tab    = BacktestTab()
        self._opt_tab   = OptimizerTab()
        # Wire "Apply Best Params" from optimizer → backtest
        self._opt_tab.apply_params.connect(self._bt_tab.apply_optimizer_params)
        self._train_tab = TrainingDataTab()
        self._cfg_tab   = ConfigTab()

        tabs.addTab(self._dash_tab,  "  Dashboard  ")
        tabs.addTab(self._live_tab,  "  Live Pipeline  ")
        tabs.addTab(self._bt_tab,    "  Backtest  ")
        tabs.addTab(self._opt_tab,   "  Optimizer  ")
        tabs.addTab(self._train_tab, "  Training Data  ")
        tabs.addTab(self._cfg_tab,   "  Config  ")

        self.setCentralWidget(tabs)

    def _on_pipeline_state(self, running: bool):
        if running:
            self._pipeline_status.setText("Pipeline: Running")
            self._pipeline_status.setStyleSheet("color: #a6e3a1; font-weight: bold; padding: 0 8px;")
            self._status_bar.showMessage("Live pipeline started")
        else:
            self._pipeline_status.setText("Pipeline: Stopped")
            self._pipeline_status.setStyleSheet("color: #f38ba8; font-weight: bold; padding: 0 8px;")
            self._status_bar.showMessage("Live pipeline stopped")
        self._dash_tab.update_pipeline_state(running)


# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    app = QApplication(sys.argv)
    app.setApplicationName("SAMgovArby")
    app.setStyle("Fusion")
    app.setStyleSheet(DARK_QSS)

    palette = QPalette()
    palette.setColor(QPalette.ColorRole.Window,      QColor("#1e1e2e"))
    palette.setColor(QPalette.ColorRole.WindowText,  QColor("#cdd6f4"))
    palette.setColor(QPalette.ColorRole.Base,        QColor("#313244"))
    palette.setColor(QPalette.ColorRole.AlternateBase, QColor("#1e1e2e"))
    palette.setColor(QPalette.ColorRole.Text,        QColor("#cdd6f4"))
    palette.setColor(QPalette.ColorRole.Button,      QColor("#313244"))
    palette.setColor(QPalette.ColorRole.ButtonText,  QColor("#cdd6f4"))
    palette.setColor(QPalette.ColorRole.Highlight,   QColor("#89b4fa"))
    palette.setColor(QPalette.ColorRole.HighlightedText, QColor("#1e1e2e"))
    palette.setColor(QPalette.ColorRole.ToolTipBase, QColor("#313244"))
    palette.setColor(QPalette.ColorRole.ToolTipText, QColor("#cdd6f4"))
    app.setPalette(palette)

    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
