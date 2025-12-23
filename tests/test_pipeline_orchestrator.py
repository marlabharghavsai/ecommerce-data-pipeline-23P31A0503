import importlib

def test_orchestrator_import():
    module = importlib.import_module("scripts.pipeline_orchestrator")
    assert module is not None
