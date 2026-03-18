"""DUPer TUI entry point. Delegates to tui/duper-tui.py."""

import sys
from pathlib import Path


def main():
    """Entry point for duper-tui command."""
    # The full TUI is in tui/duper-tui.py - import and run it
    tui_path = Path(__file__).parent.parent / "tui"
    sys.path.insert(0, str(tui_path))

    try:
        from importlib import import_module
        # Try importing the TUI module
        tui_file = tui_path / "duper-tui.py"
        if tui_file.exists():
            import importlib.util
            spec = importlib.util.spec_from_file_location("duper_tui", str(tui_file))
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            if hasattr(mod, 'main'):
                mod.main()
            elif hasattr(mod, 'DuperTUI'):
                mod.DuperTUI().run()
            else:
                print("TUI module loaded but no main() or DuperTUI class found")
        else:
            print(f"TUI not found at {tui_file}")
            print("Run from the DUPer source directory or install with: pip install -e .")
            sys.exit(1)
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("Install with: pip install textual rich aiohttp")
        sys.exit(1)


if __name__ == "__main__":
    main()
