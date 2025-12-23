import importlib

def test_cleanup_import():
    module = importlib.import_module("scripts.cleanup_old_data")
    assert module is not None
