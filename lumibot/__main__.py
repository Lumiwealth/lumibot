"""Allow `python -m lumibot` to reach the command line interface."""

from lumibot.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
