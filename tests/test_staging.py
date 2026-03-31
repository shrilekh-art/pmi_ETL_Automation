import os

def test_staging_output_exists():
    path = "data/staging/policy"
    assert os.path.exists(path), "Staging output not created"