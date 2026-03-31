import os

def test_file_moved_to_landing():
    base_path = "data/raw"
    found = False

    for root, dirs, files in os.walk(base_path):
        if "policy_20260331.csv" in files:
            found = True
            break

    assert found, "File not found in landing zone"