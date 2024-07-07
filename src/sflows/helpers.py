from collections.abc import Iterable
from typing import Any


def filter_dict_by_keys(d: dict[str, Any], keys: Iterable[str]) -> dict[str, Any]:
    return {k: v for k, v in d.items() if k in keys}
