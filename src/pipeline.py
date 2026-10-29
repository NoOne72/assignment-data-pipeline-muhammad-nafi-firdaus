"""
Data Preparation & Pipeline Project - Dataset Otomotif

Alur ETL:
    Extract   : load_data()      -> baca CSV mentah dari data/raw/
    Transform : inspect_data()   -> periksa kondisi awal dataset
                clean_data()     -> bersihkan dataset
                transform_data() -> Min-Max Scaling + encoding
    Load      : save_data()      -> simpan hasil ke data/processed/

Jalankan dari folder utama proyek:

    python src/pipeline.py

Input : data/raw/automobileEDA_dirty_training.csv
Output: data/processed/automobileEDA_processed.csv
"""
from __future__ import annotations
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

RAW_PATH = "data/raw/automobileEDA_dirty_training.csv"
PROCESSED_PATH = "data/processed/automobileEDA_processed.csv"

# Kolom transaction_date berisi 4 format tanggal yang berbeda.
# pd.to_datetime(format="mixed") tidak dipakai karena semua tanggal dianggap
# bulan-hari, padahal format "/" di data ini adalah hari-bulan.
DATE_FORMATS = {
    "%Y-%m-%d": r"^\d{4}-\d{2}-\d{2}$",         # 2025-01-01
    "%d/%m/%Y": r"^\d{2}/\d{2}/\d{4}$",         # 02/01/2025 (hari-bulan)
    "%d-%b-%Y": r"^\d{2}-[A-Za-z]{3}-\d{4}$",   # 04-Jan-2025
    "%m-%d-%Y": r"^\d{2}-\d{2}-\d{4}$",         # 01-03-2025 (bulan-hari)
}

KOLOM_KATEGORI = [
    "make", "aspiration", "num-of-doors", "body-style", "drive-wheels",
    "engine-location", "engine-type", "num-of-cylinders", "fuel-system",
    "horsepower-binned",
]

# Kolom numerik yang di-scaling (length & width sudah 0-1, symboling = rating,
# price = target, diesel & gas sudah 0/1)
KOLOM_SCALING = [
    "normalized-losses", "wheel-base", "height", "curb-weight", "engine-size",
    "bore", "stroke", "compression-ratio", "horsepower", "peak-rpm",
    "city-mpg", "highway-mpg", "city-L/100km",
]


def parse_dates(kolom: pd.Series) -> pd.Series:
    """Ubah teks dengan 4 format tanggal menjadi satu tipe datetime."""
    hasil = pd.Series(pd.NaT, index=kolom.index, dtype="datetime64[ns]")
    for fmt, pola in DATE_FORMATS.items():
        cocok = kolom.str.match(pola).fillna(False).astype(bool)
        hasil[cocok] = pd.to_datetime(kolom[cocok], format=fmt)
    return hasil


# --------------------------------------------------------------------------- #
# EXTRACT
# --------------------------------------------------------------------------- #
def load_data(filepath: str) -> pd.DataFrame:
    """Baca CSV lokal ke DataFrame."""
    df = pd.read_csv(filepath)
    print(f"[load]  {filepath} -> {df.shape[0]} baris, {df.shape[1]} kolom")
    return df


# --------------------------------------------------------------------------- #
# TRANSFORM 1: inspeksi
# --------------------------------------------------------------------------- #
def inspect_data(df: pd.DataFrame) -> None:
    """Periksa kondisi awal dataset (hanya mencetak, tidak mengubah data)."""
    print("\n===== INSPEKSI DATA =====")

    print("\n1) Lima baris pertama:")
    print(df.head().to_string())

    print(f"\n2) Jumlah baris dan kolom: {df.shape[0]} baris x {df.shape[1]} kolom")

    print("\n3) Nama kolom:")
    print(list(df.columns))

    print("\n4) Tipe data tiap kolom:")
    print(df.dtypes.to_string())

    print("\n5) Missing values per kolom:")
    missing = df.isna().sum()
    print(missing[missing > 0].to_string())
    print(f"Total missing values: {missing.sum()}")

    print(f"\n6) Jumlah baris duplikat: {df.duplicated().sum()}")

    print("\n7) Nilai unik kolom kategorikal (cek penulisan yang tidak konsisten):")
    for kolom in KOLOM_KATEGORI:
        print(f"- {kolom}: {sorted(df[kolom].dropna().unique())}")

    print("\n8) Format kolom transaction_date (bertipe teks, formatnya campur):")
    for fmt, pola in DATE_FORMATS.items():
        jumlah = df["transaction_date"].str.match(pola).sum()
        print(f"- {fmt}: {jumlah} baris")
    print(f"- kosong: {df['transaction_date'].isna().sum()} baris")

    print("\n9) Rentang nilai kolom numerik (skala tidak seragam):")
    print(df.describe().loc[["min", "max"]].T.to_string())


# --------------------------------------------------------------------------- #
# TRANSFORM 2: cleaning
# --------------------------------------------------------------------------- #
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Format + duplicates + missing values."""
    df = df.copy()  # data mentah tidak diubah
    before = len(df)
    missing_before = df.isna().sum().sum()

    # --- Format konsistensi ---
    # Teks: hapus spasi berlebih dan samakan huruf kecil
    for kolom in KOLOM_KATEGORI:
        df[kolom] = df[kolom].str.strip().str.lower()

    # Tanggal: 4 format -> 1 tipe datetime
    df["transaction_date"] = parse_dates(df["transaction_date"])
    # Tanggal kosong: data berurutan 1 hari per baris, jadi isi dari baris tetangga
    df["transaction_date"] = df["transaction_date"].interpolate()

    # --- Duplicates (dicek setelah teks dan tanggal rapi) ---
    duplikat = df.duplicated().sum()
    df = df.drop_duplicates()

    # --- Missing values ---
    # price = target, jadi barisnya dibuang (tidak diisi)
    price_kosong = df["price"].isna().sum()
    df = df.dropna(subset=["price"])

    # horsepower-binned: dihitung ulang dari horsepower
    # (batas dari data: low <= 101, medium <= 155, high >= 156)
    bin_dari_hp = pd.cut(df["horsepower"], bins=[0, 101, 155, 1000],
                         labels=["low", "medium", "high"]).astype("string")
    df["horsepower-binned"] = df["horsepower-binned"].fillna(bin_dari_hp)

    # Numerik: median (tahan terhadap outlier)
    df["horsepower"] = df["horsepower"].fillna(df["horsepower"].median())
    df["stroke"] = df["stroke"].fillna(df["stroke"].median())

    # Kategori: make -> "unknown" (modus akan mengarang merek), sisanya modus
    df["make"] = df["make"].fillna("unknown")
    df["num-of-doors"] = df["num-of-doors"].fillna(df["num-of-doors"].mode()[0])

    df = df.reset_index(drop=True)

    print(f"\n[clean] baris: {before} -> {len(df)} ({before - len(df)} dibuang)")
    print(f"[clean] duplikat dihapus: {duplikat}")
    print(f"[clean] baris dibuang karena price kosong: {price_kosong}")
    print(f"[clean] missing values: {missing_before} -> {df.isna().sum().sum()}")
    print("[clean] kolom yang berubah: transaction_date, make, aspiration, "
          "num-of-doors, body-style, drive-wheels, engine-location, engine-type, "
          "num-of-cylinders, fuel-system, horsepower-binned, horsepower, stroke")
    return df


# --------------------------------------------------------------------------- #
# TRANSFORM 3: transformasi
# --------------------------------------------------------------------------- #
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Scaling numerik + encoding kategori."""
    df = df.copy()

    # --- Min-Max Scaling ---
    scaler = MinMaxScaler()
    kolom_baru = [f"{k}_scaled" for k in KOLOM_SCALING]
    df[kolom_baru] = scaler.fit_transform(df[KOLOM_SCALING])

    # --- Ordinal encoding (kategori yang punya urutan / berupa angka) ---
    kata_ke_angka = {"two": 2, "three": 3, "four": 4, "five": 5,
                     "six": 6, "eight": 8, "twelve": 12}
    df["num-of-doors_num"] = df["num-of-doors"].map(kata_ke_angka)
    df["num-of-cylinders_num"] = df["num-of-cylinders"].map(kata_ke_angka)
    df["horsepower-binned_ordinal"] = df["horsepower-binned"].map(
        {"low": 0, "medium": 1, "high": 2}
    )

    # --- One-hot encoding (kategori tanpa urutan, jumlah kategori sedikit) ---
    kolom_onehot = ["aspiration", "body-style", "drive-wheels",
                    "engine-location", "engine-type", "fuel-system"]
    dummies = pd.get_dummies(df[kolom_onehot], prefix=kolom_onehot, dtype=int)
    df = pd.concat([df, dummies], axis=1)

    # --- Frequency encoding (make punya banyak kategori) ---
    df["make_freq"] = df["make"].map(df["make"].value_counts(normalize=True))

    # --- Perbandingan sebelum dan sesudah ---
    print("\n[transform] Min-Max Scaling (sebelum -> sesudah):")
    print(df[["normalized-losses", "normalized-losses_scaled",
              "wheel-base", "wheel-base_scaled"]].head().round(4).to_string())
    print("\n[transform] Encoding (sebelum -> sesudah):")
    print(df[["num-of-doors", "num-of-doors_num",
              "horsepower-binned", "horsepower-binned_ordinal",
              "make", "make_freq", "body-style", "body-style_sedan"]].head().round(4).to_string())

    print(f"\n[transform] {df.shape[1]} kolom setelah transformasi")
    return df


# --------------------------------------------------------------------------- #
# LOAD
# --------------------------------------------------------------------------- #
def save_data(df: pd.DataFrame, filepath: str) -> None:
    """Simpan hasil ke CSV baru di data/processed/."""
    df.to_csv(filepath, index=False)
    print(f"\n[save]  {filepath} -> {df.shape[0]} baris, {df.shape[1]} kolom")


def main() -> None:
    df_raw = load_data(RAW_PATH)
    inspect_data(df_raw)
    df = clean_data(df_raw)
    df = transform_data(df)
    save_data(df, PROCESSED_PATH)
    print("Raw file tidak berubah:", pd.read_csv(RAW_PATH).equals(df_raw))
    print("Pipeline selesai ✓")


if __name__ == "__main__":
    main()