"""Runs ``lake_build`` in its own process so a test can interrupt it.

``python lake_build_driver.py INPUT LAKE CONTROL``: the source ``slow`` has
datasets ``a`` (quick) and ``b`` (hangs, with a grandchild process).
"""

import logging
import sys
from pathlib import Path

from lake_build_fakes import FakeConverter

from quantilica.analytics.lake_build import lake_build


def main() -> None:
    input_dir, lake_dir, control = (Path(a) for a in sys.argv[1:4])
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    converter = FakeConverter("slow", ["a", "b"], control, {"b": "hang"})
    lake_build(
        input_dir,
        lake_dir,
        workers=2,
        converters={"slow": converter},
        check_disk=False,
        prevent_sleep=False,
    )


if __name__ == "__main__":
    main()
