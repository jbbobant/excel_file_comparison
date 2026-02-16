import os
import glob
import argparse
import xlsxwriter
import polars as pl
from dataclasses import dataclass
from typing import Tuple, List, Dict, Optional

# --- Configuration & Data Structures ---

@dataclass
class CompareResult:
    status: str
    match_rate: float
    details: str = ""
    shape1: Tuple[int, int] = (0, 0)
    shape2: Tuple[int, int] = (0, 0)

class Config:
    MAX_EXCEL_ROWS = 1000000  # Leaving a safe margin for headers (limit is 1,048,576)
    MISMATCH_THRESHOLD = 0.10   # 10%

# --- Core Logic Classes ---

class ReportGenerator:
    """Handles generating the report.txt and differences.xlsx files."""
    
    @staticmethod
    def write_report_txt(output_dir: str, file_name: str, results: Dict[str, CompareResult]):
        report_path = os.path.join(output_dir, "report.txt")
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(f"--- Comparison Report for: {file_name} ---\n\n")
            for sheet, result in results.items():
                f.write(f"Sheet: {sheet}\n")
                f.write(f"Shape V1: {result.shape1} | Shape V2: {result.shape2}\n")
                f.write(f"Status: {result.status}\n")
                f.write(f"Match Rate: {result.match_rate * 100:.2f}%\n")
                if result.details:
                    f.write(f"Details: {result.details}\n")
                f.write("-" * 40 + "\n")

    @staticmethod
    def write_differences_xlsx(output_dir: str, diff_df: pl.DataFrame):
        if diff_df.is_empty():
            return
            
        diff_path = os.path.join(output_dir, "differences.xlsx")
        
        # Handle Excel Row Limits by chunking into multiple sheets if necessary
        with xlsxwriter.Workbook(diff_path) as workbook:
            total_rows = diff_df.height
            for i in range(0, total_rows, Config.MAX_EXCEL_ROWS):
                chunk = diff_df.slice(i, Config.MAX_EXCEL_ROWS)
                sheet_name = f"Diff_{i // Config.MAX_EXCEL_ROWS + 1}"
                chunk.write_excel(workbook=workbook, worksheet=sheet_name)


class SheetComparator:
    """Handles the hierarchical early-stopping logic for two specific sheets."""
    
    def __init__(self, use_hash: bool):
        self.use_hash = use_hash

    def compare(self, df1: pl.DataFrame, df2: pl.DataFrame) -> Tuple[CompareResult, Optional[pl.DataFrame]]:
        shape1, shape2 = df1.shape, df2.shape
        
        # 1. Shape Match
        if shape1 != shape2:
            return CompareResult("Shape mismatch", 0.0, f"V1: {shape1}, V2: {shape2}", shape1, shape2), None
            
        # 2. Column Names Match
        if df1.columns != df2.columns:
            diff_cols = set(df1.columns) ^ set(df2.columns)
            return CompareResult("Column names mismatch", 0.0, f"Diff cols: {diff_cols}", shape1, shape2), None
            
        # 3. Data Types Match
        if df1.dtypes != df2.dtypes:
            type_diff = {c: (str(t1), str(t2)) for c, t1, t2 in zip(df1.columns, df1.dtypes, df2.dtypes) if t1 != t2}
            return CompareResult("Column type mismatch", 0.0, f"Type diffs: {type_diff}", shape1, shape2), None

        # 4. Empty Files Check
        if shape1[0] == 0:
            return CompareResult("Perfect match", 1.0, "Files are empty (Headers only)", shape1, shape2), None

        # 5. Row by Row Comparison
        return self._compare_data(df1, df2, shape1, shape2)

    def _compare_data(self, df1: pl.DataFrame, df2: pl.DataFrame, shape1: tuple, shape2: tuple) -> Tuple[CompareResult, Optional[pl.DataFrame]]:
        if self.use_hash:
            h1, h2 = df1.hash_rows(), df2.hash_rows()
            mismatch_mask = h1 != h2
        else:
            # Build an expression to check for any mismatch across all columns, handling Nulls safely
            df2_renamed = df2.rename({c: f"{c}_v2" for c in df2.columns})
            combined = pl.concat([df1, df2_renamed], how="horizontal")
            
            exprs = []
            for c in df1.columns:
                c1, c2 = pl.col(c), pl.col(f"{c}_v2")
                # True if one is null and other is not, OR if their values differ
                is_diff = (
                    (c1.is_null() & c2.is_not_null()) |
                    (c1.is_not_null() & c2.is_null()) |
                    ((c1 != c2).fill_null(False))
                )
                exprs.append(is_diff)
                
            mismatch_mask = combined.select(pl.any_horizontal(exprs)).to_series()

        mismatch_count = mismatch_mask.sum()
        total_rows = df1.height
        match_rate = (total_rows - mismatch_count) / total_rows

        if mismatch_count == 0:
            return CompareResult("Perfect match", 1.0, "", shape1, shape2), None
            
        if (mismatch_count / total_rows) > Config.MISMATCH_THRESHOLD:
            return CompareResult(f"Data mismatch >{Config.MISMATCH_THRESHOLD*100}%", match_rate, f"{mismatch_count} rows differ.", shape1, shape2), None

        # 6. Generate Differences Output (Melt / Unpivot)
        diff_df = self._generate_differences_df(df1, df2, mismatch_mask)
        return CompareResult("Data mismatch", match_rate, f"{mismatch_count} rows differ.", shape1, shape2), diff_df

    def _generate_differences_df(self, df1: pl.DataFrame, df2: pl.DataFrame, mismatch_mask: pl.Series) -> pl.DataFrame:
        df1_idx = df1.with_row_index("Row Index").filter(mismatch_mask)
        df2_idx = df2.with_row_index("Row Index").filter(mismatch_mask)

        # Cast to string for reliable unpivoting
        df1_str = df1_idx.select(["Row Index"] + [pl.col(c).cast(pl.Utf8) for c in df1.columns])
        df2_str = df2_idx.select(["Row Index"] + [pl.col(c).cast(pl.Utf8) for c in df2.columns])

        # Unpivot (Note: replace 'unpivot' with 'melt' if using Polars < 0.20)
        melt1 = df1_str.unpivot(index="Row Index", variable_name="Column Name", value_name="Value V1")
        melt2 = df2_str.unpivot(index="Row Index", variable_name="Column Name", value_name="Value V2")

        diff_df = melt1.join(melt2, on=["Row Index", "Column Name"])

        # Keep only cells that actually differ
        diff_df = diff_df.filter(
            (pl.col("Value V1") != pl.col("Value V2")).fill_null(True) & 
            ~(pl.col("Value V1").is_null() & pl.col("Value V2").is_null())
        )

        # Attach Column Type
        schema_df = pl.DataFrame({"Column Name": list(df1.columns), "Column type": [str(t) for t in df1.dtypes]})
        diff_df = diff_df.join(schema_df, on="Column Name")
        
        return diff_df.select(["Row Index", "Column Name", "Column type", "Value V1", "Value V2"]).sort(["Row Index", "Column Name"])


class FileComparator:
    """Manages the extraction and sheet-level comparison of two Excel files."""
    
    def __init__(self, use_hash: bool):
        self.sheet_comparator = SheetComparator(use_hash)

    def process_pair(self, file1_path: str, file2_path: str, output_dir: str):
        file_name1 = os.path.basename(file1_path)
        file_name2 = os.path.basename(file2_path)

        # 0. File Name Match
        if file_name1 != file_name2:
            self._print_row(file_name1, "N/A", "N/A", "File name mismatch", f"V1: {file_name1} != V2: {file_name2}")
            return

        # Read Sheets using Calamine
        try:
            sheets1 = pl.read_excel(file1_path, engine="calamine", sheet_id=None)
            sheets2 = pl.read_excel(file2_path, engine="calamine", sheet_id=None)
        except Exception as e:
            self._print_row(file_name1, "ERROR", "ERROR", "Read Error", str(e))
            return

        # 1. Sheet Names Match
        if set(sheets1.keys()) != set(sheets2.keys()):
            diff = set(sheets1.keys()) ^ set(sheets2.keys())
            self._print_row(file_name1, "N/A", "N/A", "Sheet names mismatch", f"Mismatched sheets: {diff}")
            return

        results = {}
        all_diffs = []

        for sheet_name in sheets1.keys():
            result, diff_df = self.sheet_comparator.compare(sheets1[sheet_name], sheets2[sheet_name])
            results[sheet_name] = result
            
            self._print_row(f"{file_name1} [{sheet_name}]", str(result.shape1), str(result.shape2), result.status, result.details)

            if diff_df is not None:
                diff_df = diff_df.with_columns(pl.lit(sheet_name).alias("Sheet Name"))
                all_diffs.append(diff_df)

        # Output Generation
        ReportGenerator.write_report_txt(output_dir, file_name1, results)
        if all_diffs:
            final_diff = pl.concat(all_diffs, how="vertical")
            final_diff = final_diff.select(["Sheet Name", "Row Index", "Column Name", "Column type", "Value V1", "Value V2"])
            ReportGenerator.write_differences_xlsx(output_dir, final_diff)

    def _print_row(self, name: str, shape1: str, shape2: str, status: str, details: str):
        print(f"{name:<30} | {shape1:<15} | {shape2:<15} | {status:<25} | {details}")


class App:
    """CLI orchestrator that parses folders and runs the comparisons."""
    
    def __init__(self, folder_path: str, use_hash: bool):
        self.folder_path = folder_path
        self.file_comparator = FileComparator(use_hash)

    def run(self):
        print("\n" + "="*120)
        print(f"{'Target':<30} | {'Shape V1':<15} | {'Shape V2':<15} | {'Status':<25} | {'Details'}")
        print("="*120)

        for item in os.listdir(self.folder_path):
            file_dir = os.path.join(self.folder_path, item)
            if not os.path.isdir(file_dir):
                continue

            # Dynamically detect the two subdirectories
            subdirs = [os.path.join(file_dir, d) for d in os.listdir(file_dir) if os.path.isdir(os.path.join(file_dir, d))]
            if len(subdirs) != 2:
                print(f"{item:<30} | {'N/A':<15} | {'N/A':<15} | {'Folder Error':<25} | Expected 2 subfolders, found {len(subdirs)}")
                continue

            subdirs.sort()  # Ensures consistent V1/V2 ordering based on folder names
            
            # Find the xlsx files inside the subdirectories
            file1_list = glob.glob(os.path.join(subdirs[0], "*.xlsx"))
            file2_list = glob.glob(os.path.join(subdirs[1], "*.xlsx"))

            if not file1_list or not file2_list:
                print(f"{item:<30} | {'N/A':<15} | {'N/A':<15} | {'Missing File':<25} | Missing .xlsx in one or both subfolders.")
                continue

            # Process the pair
            self.file_comparator.process_pair(file1_list[0], file2_list[0], file_dir)

        print("="*120 + "\nComparison Complete.\n")

# --- Execution ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Optimized XLSX Comparator using Polars and Calamine.")
    parser.add_argument("folder_path", type=str, help="Path to the main folder containing the file directories.")
    parser.add_argument("--hash", action="store_true", help="Use native Polars row hashing for faster comparison.")
    
    args = parser.parse_argument()
    
    app = App(args.folder_path, args.hash)
    app.run()