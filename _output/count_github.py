import pandas as pd

files = ["question_tags.csv", "questions.csv"]
count = 0

for file in files:
    try:
        print(f"Processing {file}...")
        with open("_output/github_count.txt", "a") as f:
            f.write(f"Processing {file}...\n")
        df = pd.read_csv(file, dtype=str, on_bad_lines="skip")
        for i in range(0, len(df), 100000):
            chunk = df.iloc[i:i+100000]
            chunk_count = chunk.apply(lambda row: row.astype(str).str.contains("GitHub", case=False, na=False).any(), axis=1).sum()
            count += chunk_count
            print(f"Processed {i+100000} rows, current total: {count}")
            with open("_output/github_count.txt", "a") as f:
                f.write(f"Processed {i+100000} rows, current total: {count}\n")
    except FileNotFoundError:
        print(f"Warning: {file} not found.")
    except Exception as e:
        print(f"Error processing {file}: {e}")

print(f"Total lines containing 'GitHub': {count}")

with open("_output/github_count.txt", "a") as f:
    f.write(f"Total lines containing 'GitHub': {count}\n")

