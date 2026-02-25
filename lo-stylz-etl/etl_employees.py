import csv
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
SOURCE_FILE = BASE_DIR / "employees_source.csv"
OUTPUT_FILE = BASE_DIR / "employees.csv"


def normalize_phone(phone: str) -> str:
    digits = "".join(ch for ch in phone if ch.isdigit())
    if len(digits) == 7:
        return f"555-{digits[-4:]}"
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return phone.strip()


def parse_active_flag(value: str) -> str:
    return "Active" if value.strip().lower() in {"yes", "true", "1", "active"} else "Inactive"


def normalize_date(value: str) -> str:
    parsed = datetime.strptime(value.strip(), "%Y-%m-%d")
    return parsed.strftime("%Y-%m-%d")


def transform_row(row: dict) -> dict:
    first_name = row["first_name"].strip().title()
    last_name = row["last_name"].strip().title()
    return {
        "employee_id": row["employee_id"].strip(),
        "full_name": f"{first_name} {last_name}",
        "email": row["email"].strip().lower(),
        "phone": normalize_phone(row["phone"]),
        "role": row["role"].strip().title(),
        "hire_date": normalize_date(row["hire_date"]),
        "status": parse_active_flag(row["is_active"]),
        "hourly_rate": f"{float(row['hourly_rate']):.2f}",
        "commission_rate": f"{float(row['commission_rate']):.2f}",
        "specialty": row["specialty"].strip().title(),
    }


def run_etl(source_file: Path = SOURCE_FILE, output_file: Path = OUTPUT_FILE) -> None:
    if not source_file.exists():
        raise FileNotFoundError(f"Source file not found: {source_file}")

    transformed_rows = []
    with source_file.open("r", newline="", encoding="utf-8") as src:
        reader = csv.DictReader(src)
        for row in reader:
            transformed_rows.append(transform_row(row))

    fieldnames = [
        "employee_id",
        "full_name",
        "email",
        "phone",
        "role",
        "hire_date",
        "status",
        "hourly_rate",
        "commission_rate",
        "specialty",
    ]

    with output_file.open("w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(transformed_rows)

    print(f"ETL complete. Output file created: {output_file}")


if __name__ == "__main__":
    run_etl()
