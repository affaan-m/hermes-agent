"""Vendored pinned fixture inputs for the native-caller composition tests.

These files are byte-exact copies of reviewed sources; PINS.json records each
SHA-256. They are test fixtures only, never imported by production code, and
pytest must not collect the vendored test modules directly (the ported
adapter test loads them by path after verifying the pins).
"""

collect_ignore_glob = ["native_caller/*"]
