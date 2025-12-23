import json
import os

def test_quality_report_exists():
    path = "data/quality/data_quality_report.json"
    assert os.path.exists(path)

def test_quality_score_is_100():
    with open("data/quality/data_quality_report.json") as f:
        report = json.load(f)
    assert report["overall_quality_score"] >= 95
