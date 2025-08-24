import os
import sys
import zipfile
import json

def extract_zip(zip_path, output_dir):
    if not zipfile.is_zipfile(zip_path):
        raise ValueError(f"Not a ZIP file: {zip_path}")
    os.makedirs(output_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(output_dir)
    os.remove(zip_path)

def fix_json(input_path, output_path): 
    try:
        with open(input_path, 'r') as f:
            data = json.load(f)
        with open(output_path, 'w') as f:
            for key, value in data.items():
                f.write(json.dumps({key: value}) + '\n')
        os.remove(input_path)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {input_path}. Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("❌ Usage: python execute.py <ZIP_FILE_PATH>")
        sys.exit(1)

    zip_path = os.path.abspath(sys.argv[1])

    if not os.path.exists(zip_path):
        print(f"❌ ZIP file not found: {zip_path}")
        sys.exit(1)

    # Folder where ZIP resides
    folder_path = os.path.dirname(zip_path)
    # Subfolder for extracted files
    extract_subfolder = os.path.join(folder_path, "Extracted_Data")

    print(f"Extracting to: {extract_subfolder}")
    print(f"ZIP path: {zip_path}")

    try:
        extract_zip(zip_path, extract_subfolder)
        print("✅ Extraction complete")

    except Exception as e:
        print(f"❌ Failed: {e}", file=sys.stderr)
        sys.exit(1)
