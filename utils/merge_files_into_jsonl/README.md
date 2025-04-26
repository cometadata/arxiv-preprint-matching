# Merge Files into JSONL

Script to merge multiple JSON files from a directory into a single JSONL file.

## Features
- Processes JSON files in a specified directory (including subdirectories)
- Converts each JSON file into a single line in the output JSONL file
- Maintains original JSON structure while eliminating whitespace

## Usage
```
python merge_files_into_jsonl.py -i INPUT_DIRECTORY -o OUTPUT_FILE.jsonl
```

## Output
The script provides a summary of processing results, including:
- Number of files successfully processed
- Number of files with processing errors