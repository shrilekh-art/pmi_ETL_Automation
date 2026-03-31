import os

def test_curated_output_exists():
    path = "data/curated/policy/policy_curated.csv"
    assert os.path.exists(path), "Curated output not created"