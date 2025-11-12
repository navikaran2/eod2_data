import os
import polars as pl

print("=" * 60)
print("🚀 NSE Data Merge to Parquet - Started")
print("=" * 60)

# 📁 Paths (GitHub Actions compatible)
input_folder = "daily"
output_parquet = "Master_nse_data.parquet"

print(f"📁 Output file: {output_parquet}")

# 📋 Fixed base columns and dtypes
schema_overrides = {
    "Date": pl.Date,
    "Open": pl.Float64,
    "High": pl.Float64,
    "Low": pl.Float64,
    "Close": pl.Float64,
    "Volume": pl.Float64,
    "Series": pl.Utf8,
    "TOTAL_TRADES": pl.Float64,
    "QTY_PER_TRADE": pl.Float64,
    "DLV_QTY": pl.Float64,
}

base_columns = list(schema_overrides.keys())

# 📂 Collect all CSV files
print(f"\n📂 Scanning folder: {input_folder}")
files = [f for f in os.listdir(input_folder) if f.endswith(".csv")]
print(f"📊 Found {len(files)} CSV files")

if len(files) == 0:
    print("❌ Error: No CSV files found in daily/ folder!")
    exit(1)

frames = []
failed_files = []

# 🔄 Read & merge all CSVs
print(f"\n🔄 Processing CSV files...")
for i, file in enumerate(files, 1):
    file_path = os.path.join(input_folder, file)
    symbol = file.replace(".csv", "").upper()

    try:
        df = pl.read_csv(
            file_path,
            ignore_errors=True,
            infer_schema_length=1000,
            null_values=["nan", "NaN", "N/A", "null", ""],
            schema_overrides=schema_overrides
        )

        # Keep only required columns
        df = df.select([col for col in base_columns if col in df.columns])

        # Add SYMBOL column
        df = df.with_columns(pl.lit(symbol).alias("SYMBOL"))

        frames.append(df)
        
        # Progress indicator (every 100 files)
        if i % 100 == 0:
            print(f"  ✓ Processed {i}/{len(files)} files...")
            
    except Exception as e:
        failed_files.append((file, str(e)))
        print(f"  ⚠️ Skipped {file}: {e}")

print(f"\n✅ Successfully processed: {len(frames)} files")
if failed_files:
    print(f"⚠️ Failed files: {len(failed_files)}")
    for fname, err in failed_files[:5]:  # Show first 5 failures
        print(f"  - {fname}: {err}")

if len(frames) == 0:
    print("❌ Error: No valid data frames created!")
    exit(1)

# 🧩 Concatenating all frames
print(f"\n🧩 Concatenating {len(frames)} dataframes...")
merged_df = pl.concat(frames, how="vertical_relaxed", rechunk=True)

# 📋 Ensure column order & dtypes
print("📋 Standardizing columns and data types...")
merged_df = merged_df.select([
    pl.col("Date").cast(pl.Date),
    pl.col("Open").cast(pl.Float64),
    pl.col("High").cast(pl.Float64),
    pl.col("Low").cast(pl.Float64),
    pl.col("Close").cast(pl.Float64),
    pl.col("Volume").cast(pl.Float64),
    pl.col("Series").cast(pl.Utf8),
    pl.col("TOTAL_TRADES").cast(pl.Float64),
    pl.col("QTY_PER_TRADE").cast(pl.Float64),
    pl.col("DLV_QTY").cast(pl.Float64),
    pl.col("SYMBOL").cast(pl.Utf8),
])

# 📊 Statistics
print(f"\n📊 Final Dataset Statistics:")
print(f"  - Total rows: {len(merged_df):,}")
print(f"  - Total columns: {len(merged_df.columns)}")
print(f"  - Unique symbols: {merged_df['SYMBOL'].n_unique()}")
print(f"  - Date range: {merged_df['Date'].min()} to {merged_df['Date'].max()}")

# 💾 Writing to Parquet
print(f"\n💾 Writing to Parquet: {output_parquet}")
merged_df.write_parquet(output_parquet)

# ✅ Verify file created
if os.path.exists(output_parquet):
    file_size = os.path.getsize(output_parquet)
    file_size_mb = file_size / (1024 * 1024)
    print(f"✅ Success! File created: {output_parquet}")
    print(f"📦 File size: {file_size_mb:.2f} MB")
else:
    print(f"❌ Error: File not created!")
    exit(1)

print("\n" + "=" * 60)
print("✨ NSE Data Merge Completed Successfully!")
print("=" * 60)
