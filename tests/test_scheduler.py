import importlib

def test_scheduler_import():
    module = importlib.import_module("scripts.scheduler")
    assert module is not None
