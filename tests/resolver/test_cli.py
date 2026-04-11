"""tests/resolver/test_cli.py — CLI smoke test."""
import os, sys, tempfile, subprocess

SAMPLE_CSV = (
    "PIID,Modification Number,Date Signed,Fiscal Year,Action Obligation,"
    "Ultimate Parent Unique Entity ID,Unique Entity ID,CAGE Code,"
    "Legal Business Name,Ultimate Parent Legal Business Name,"
    "Doing Business As Name,Contractor Name,"
    "Vendor Address State,Vendor Address Country,Country of Incorporation\n"
    "PIID001,,2023-01-15,2023,5000000,PUEI_RTX,UEI_RTX_SUB,RX001,"
    "Raytheon Intelligence and Space,RTX Corporation,,"
    "Raytheon Intelligence and Space,VA,USA,USA\n"
    "PIID002,,2023-02-20,2023,1000000,,UEI_UNKN,,Some Unknown Private Firm,,,,"
    "MD,USA,USA\n"
)


def test_cli_runs_end_to_end():
    with tempfile.TemporaryDirectory() as d:
        csv_path = os.path.join(d, "contracts.csv")
        out_dir  = os.path.join(d, "output")
        db_path  = os.path.join(d, "resolver.duckdb")
        with open(csv_path, "w") as f:
            f.write(SAMPLE_CSV)
        result = subprocess.run(
            [sys.executable, "-m", "resolver",
             "--input", csv_path,
             "--output-dir", out_dir,
             "--db", db_path,
             "--no-refresh",
             "--log-level", "WARNING"],
            capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0, result.stderr + result.stdout
        out_files = os.listdir(out_dir)
        assert any(f.endswith(".csv") for f in out_files), f"No CSV in {out_files}"
