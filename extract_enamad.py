"""Root shim — forwards to the package location."""
import sys
import pathlib

_root = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(_root / "src"))

from enamad.scraper.extract_enamad import (  # noqa: E402
    EnamadClient,
    TRUSTSEAL_LABELS,
    clean_domain,
    main,
    maybe_enrich_row,
    normalize_search_row,
)

if __name__ == "__main__":
    raise SystemExit(main())
