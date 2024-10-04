
# CSV Sensitive Data Deidentification Script

This script extracts and deidentifies sensitive information from a specified column in a CSV file. The processed text will be stored in a new column (`processed_text`), and the extracted entities will be stored as JSON in the `entities` column.

## Requirements

- Python 3
- Required libraries in `requirements.txt`

## Setup

1. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```
2. Specify the input CSV file and the output folder location in the script.

## How to Run

```bash
python3 main.py
```

## Output

The processed CSV will be saved in the specified output folder, with the deidentified text and extracted entities included.