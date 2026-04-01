import os

def get_latest_csv_file(base_path):
    latest_file = None
    latest_time = 0

    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file.endswith(".csv"):
                full_path = os.path.join(root, file)
                file_time = os.path.getmtime(full_path)

                if file_time > latest_time:
                    latest_time = file_time
                    latest_file = full_path

    return latest_file