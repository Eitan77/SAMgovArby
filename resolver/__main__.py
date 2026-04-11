"""resolver/__main__.py — Enables: python -m resolver [args]"""
from resolver.cli import build_arg_parser, run_batch

if __name__ == "__main__":
    parser = build_arg_parser()
    args   = parser.parse_args()
    run_batch(args)
