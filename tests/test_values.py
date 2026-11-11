"""How states are split into text and number."""

import pytest

from custom_components.chronicle.writer import attributes_hash, parse_value


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ("21.5", (None, 21.5)),
        ("-3", (None, -3.0)),
        ("1e3", (None, 1000.0)),
        (".5", (None, 0.5)),
        ("on", ("on", 1.0)),
        ("off", ("off", 0.0)),
        ("unavailable", ("unavailable", None)),
        ("unknown", ("unknown", None)),
        ("nan", ("nan", None)),
        ("inf", ("inf", None)),
        ("1_000", ("1_000", None)),
        ("", ("", None)),
        ("2026-11-11T10:00:00+00:00", ("2026-11-11T10:00:00+00:00", None)),
    ],
)
def test_parse_value(state, expected):
    assert parse_value(state) == expected


def test_attributes_hash_is_stable_and_signed():
    first = attributes_hash(b'{"a":1}')
    assert first == attributes_hash(b'{"a":1}')
    assert -(2**63) <= first < 2**63
    assert first != attributes_hash(b'{"a":2}')
