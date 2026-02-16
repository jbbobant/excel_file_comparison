# Optimized XLSX File Comparator

A high-performance Python CLI tool designed to compare large pairs of `.xlsx` files. Built with object-oriented programming (OOP), it leverages **Polars** for zero-copy vectorized data manipulation and **Calamine** (`fastexcel`) for blistering-fast, memory-efficient Excel parsing.

##  Features

* **Hierarchical Early-Stopping:** Optimizes compute by checking metadata before data. It sequentially verifies File Names ➔ Sheet Names ➔ Shapes ➔ Column Names ➔ Data Types ➔ Row Data.
* **Blazing Fast I/O:** Uses the Rust-based Calamine engine, avoiding the traditional bottlenecks of Python-based Excel parsers like `openpyxl`.
* **Native Row Hashing:** Optional `--hash` flag to use Polars' native row hashing for rapid equality checks.
* **10% Mismatch Threshold:** Automatically aborts row-by-row diffing if mismatches exceed 10% of the dataset, flagging it as a massive divergence to save processing time.
* **Excel Limit Handling:** Automatically chunks output into multiple sheets if the differences exceed Excel's 1,048,576 row limit.
* **Dynamic Folder Detection:** Automatically detects the two subdirectories containing the files to compare, regardless of their naming convention.

##  Prerequisites

Ensure you have Python 3.8+ installed. Install the required modern data stack dependencies:

```bash
pip install polars fastexcel xlsxwriter
```

##  Directory Structure

The script expects a root folder containing subfolders for each file comparison. Inside each file folder, there must be **exactly two subfolders** (the names don't matter; they will be sorted alphabetically to determine Version 1 and Version 2). 

Example structure:
```text
target_folder/
├── employee_data/
│   ├── export_v1/
│   │   └── employees.xlsx
│   └── export_v2/
│       └── employees.xlsx
├── financial_report/
│   ├── 2023_Q1/
│   │   └── financials.xlsx
│   └── 2023_Q2/
│       └── financials.xlsx
```

##  Usage

Run the script via the Command Line Interface (CLI).

**Basic Comparison:**
```bash
python main.py "path/to/target_folder"
```

**Optimized Comparison (using Polars native hashing):**
```bash
python main.py "path/to/target_folder" --hash
```

##  Outputs

For every pair of files processed, the script generates three types of output:

1. **Terminal Live Tracker:**
   A continuously updating table logging the shape of the files, the comparison status (e.g., `Shape mismatch`, `Perfect match`, `Data mismatch`), and specific details or error messages.

2. **`report.txt` (Saved in each root file folder, e.g., inside `employee_data/`):**
   A summary of the comparison detailing the shapes, data types, the exact failure point (if any), and a calculated match rate percentage.

3. **`differences.xlsx` (Only generated if row differences < 10%):**
   A detailed, unpivoted (melted) Excel file highlighting exact cellular differences. The format is strictly:
   | Row Index | Column Name | Column type | Value V1 | Value V2 |

##  How the Early-Stopping Logic Works

To maximize performance, the script aborts the comparison at the earliest point of failure:
1. **File Name Check:** Are the root file names identical?
2. **Sheet Name Check:** Do both files contain the exact same sheets?
3. **Shape Check:** Do the matching sheets have the exact same number of rows and columns?
4. **Column Check:** Are the column headers perfectly matched?
5. **Type Check:** Do the inferred data types for each column match?
6. **Empty File Check:** If the files have headers but 0 rows, it halts and reports a `Perfect match`.
7. **Row-by-Row Diff:** Only if all the above pass does it execute the memory-intensive row-by-row comparison.