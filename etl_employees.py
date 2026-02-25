import csv
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
EMPLOYEE_SOURCE_FILE = BASE_DIR / "employees_source.csv"
EMPLOYEE_OUTPUT_FILE = BASE_DIR / "employees.csv"
CUSTOMER_SOURCE_FILE = BASE_DIR / "customers_source.csv"
CUSTOMER_OUTPUT_FILE = BASE_DIR / "customers.csv"
APPOINTMENT_SOURCE_FILE = BASE_DIR / "appointments_source.csv"
APPOINTMENT_OUTPUT_FILE = BASE_DIR / "appointments.csv"
APPOINTMENT_ENRICHED_OUTPUT_FILE = BASE_DIR / "appointments_enriched.csv"
DAILY_SUMMARY_OUTPUT_FILE = BASE_DIR / "daily_summary.csv"


def normalize_phone(phone: str) -> str:
    digits = "".join(ch for ch in phone if ch.isdigit())
    if len(digits) == 7:
        return f"555-{digits[-4:]}"
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return phone.strip()


def parse_active_flag(value: str) -> str:
    return "Active" if value.strip().lower() in {"yes", "true", "1", "active"} else "Inactive"


def parse_marketing_opt_in(value: str) -> str:
    return "Yes" if value.strip().lower() in {"yes", "true", "1", "active"} else "No"


def normalize_date(value: str) -> str:
    parsed = datetime.strptime(value.strip(), "%Y-%m-%d")
    return parsed.strftime("%Y-%m-%d")


def normalize_datetime(value: str) -> str:
    parsed = datetime.strptime(value.strip(), "%Y-%m-%d %H:%M")
    return parsed.strftime("%Y-%m-%d %H:%M")


def normalize_appointment_status(value: str) -> str:
    status_map = {
        "completed": "Completed",
        "cancelled": "Cancelled",
        "no show": "No Show",
        "noshow": "No Show",
        "scheduled": "Scheduled",
    }
    return status_map.get(value.strip().lower(), value.strip().title())


def transform_employee_row(row: dict) -> dict:
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


def transform_customer_row(row: dict) -> dict:
    first_name = row["first_name"].strip().title()
    last_name = row["last_name"].strip().title()
    return {
        "customer_id": row["customer_id"].strip(),
        "full_name": f"{first_name} {last_name}",
        "email": row["email"].strip().lower(),
        "phone": normalize_phone(row["phone"]),
        "joined_date": normalize_date(row["joined_date"]),
        "loyalty_points": str(int(float(row["loyalty_points"]))),
        "marketing_opt_in": parse_marketing_opt_in(row["marketing_opt_in"]),
        "preferred_contact": row["preferred_contact"].strip().lower(),
    }


def transform_appointment_row(row: dict) -> dict:
    start_time = datetime.strptime(row["appointment_start"].strip(), "%Y-%m-%d %H:%M")
    end_time = datetime.strptime(row["appointment_end"].strip(), "%Y-%m-%d %H:%M")
    duration_minutes = int((end_time - start_time).total_seconds() // 60)

    return {
        "appointment_id": row["appointment_id"].strip(),
        "customer_id": row["customer_id"].strip(),
        "employee_id": row["employee_id"].strip(),
        "service_name": row["service_name"].strip().title(),
        "appointment_start": normalize_datetime(row["appointment_start"]),
        "appointment_end": normalize_datetime(row["appointment_end"]),
        "duration_minutes": str(duration_minutes),
        "status": normalize_appointment_status(row["status"]),
        "service_price": f"{float(row['service_price']):.2f}",
        "payment_method": row["payment_method"].strip().lower(),
        "tip_amount": f"{float(row['tip_amount']):.2f}",
    }


def run_single_etl(source_file: Path, output_file: Path, transform_func, fieldnames: list[str]) -> None:
    if not source_file.exists():
        raise FileNotFoundError(f"Source file not found: {source_file}")

    transformed_rows = []
    with source_file.open("r", newline="", encoding="utf-8") as src:
        reader = csv.DictReader(src)
        for row in reader:
            transformed_rows.append(transform_func(row))

    with output_file.open("w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(transformed_rows)

    print(f"ETL complete. Output file created: {output_file.name}")


def run_appointment_enrichment(
    employee_file: Path = EMPLOYEE_OUTPUT_FILE,
    customer_file: Path = CUSTOMER_OUTPUT_FILE,
    appointment_file: Path = APPOINTMENT_OUTPUT_FILE,
    output_file: Path = APPOINTMENT_ENRICHED_OUTPUT_FILE,
) -> None:
    for required_file in [employee_file, customer_file, appointment_file]:
        if not required_file.exists():
            raise FileNotFoundError(f"Required file not found for enrichment: {required_file}")

    with employee_file.open("r", newline="", encoding="utf-8") as employees_src:
        employee_reader = csv.DictReader(employees_src)
        employee_name_map = {row["employee_id"]: row["full_name"] for row in employee_reader}

    with customer_file.open("r", newline="", encoding="utf-8") as customers_src:
        customer_reader = csv.DictReader(customers_src)
        customer_name_map = {row["customer_id"]: row["full_name"] for row in customer_reader}

    enriched_rows = []
    with appointment_file.open("r", newline="", encoding="utf-8") as appointments_src:
        appointment_reader = csv.DictReader(appointments_src)
        for row in appointment_reader:
            enriched_row = {
                "appointment_id": row["appointment_id"],
                "appointment_start": row["appointment_start"],
                "appointment_end": row["appointment_end"],
                "duration_minutes": row["duration_minutes"],
                "status": row["status"],
                "service_name": row["service_name"],
                "service_price": row["service_price"],
                "tip_amount": row["tip_amount"],
                "payment_method": row["payment_method"],
                "customer_id": row["customer_id"],
                "customer_name": customer_name_map.get(row["customer_id"], "Unknown Customer"),
                "employee_id": row["employee_id"],
                "employee_name": employee_name_map.get(row["employee_id"], "Unknown Employee"),
            }
            enriched_rows.append(enriched_row)

    fieldnames = [
        "appointment_id",
        "appointment_start",
        "appointment_end",
        "duration_minutes",
        "status",
        "service_name",
        "service_price",
        "tip_amount",
        "payment_method",
        "customer_id",
        "customer_name",
        "employee_id",
        "employee_name",
    ]

    with output_file.open("w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(enriched_rows)

    print(f"ETL complete. Output file created: {output_file.name}")


def run_daily_summary(
    appointment_enriched_file: Path = APPOINTMENT_ENRICHED_OUTPUT_FILE,
    output_file: Path = DAILY_SUMMARY_OUTPUT_FILE,
) -> None:
    if not appointment_enriched_file.exists():
        raise FileNotFoundError(f"Required file not found for summary: {appointment_enriched_file}")

    summary = {}
    with appointment_enriched_file.open("r", newline="", encoding="utf-8") as src:
        reader = csv.DictReader(src)
        for row in reader:
            appointment_date = row["appointment_start"].split(" ")[0]
            key = (appointment_date, row["employee_id"], row["employee_name"])

            if key not in summary:
                summary[key] = {
                    "appointment_date": appointment_date,
                    "employee_id": row["employee_id"],
                    "employee_name": row["employee_name"],
                    "appointments_count": 0,
                    "completed_appointments": 0,
                    "service_revenue": 0.0,
                    "tip_revenue": 0.0,
                }

            current = summary[key]
            current["appointments_count"] += 1

            if row["status"].strip().lower() == "completed":
                current["completed_appointments"] += 1
                current["service_revenue"] += float(row["service_price"])
                current["tip_revenue"] += float(row["tip_amount"])

    output_rows = []
    for key in sorted(summary.keys()):
        current = summary[key]
        output_rows.append(
            {
                "appointment_date": current["appointment_date"],
                "employee_id": current["employee_id"],
                "employee_name": current["employee_name"],
                "appointments_count": str(current["appointments_count"]),
                "completed_appointments": str(current["completed_appointments"]),
                "service_revenue": f"{current['service_revenue']:.2f}",
                "tip_revenue": f"{current['tip_revenue']:.2f}",
                "total_revenue": f"{(current['service_revenue'] + current['tip_revenue']):.2f}",
            }
        )

    fieldnames = [
        "appointment_date",
        "employee_id",
        "employee_name",
        "appointments_count",
        "completed_appointments",
        "service_revenue",
        "tip_revenue",
        "total_revenue",
    ]

    with output_file.open("w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    print(f"ETL complete. Output file created: {output_file.name}")


def run_etl() -> None:
    run_single_etl(
        source_file=EMPLOYEE_SOURCE_FILE,
        output_file=EMPLOYEE_OUTPUT_FILE,
        transform_func=transform_employee_row,
        fieldnames=[
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
        ],
    )

    run_single_etl(
        source_file=CUSTOMER_SOURCE_FILE,
        output_file=CUSTOMER_OUTPUT_FILE,
        transform_func=transform_customer_row,
        fieldnames=[
            "customer_id",
            "full_name",
            "email",
            "phone",
            "joined_date",
            "loyalty_points",
            "marketing_opt_in",
            "preferred_contact",
        ],
    )

    run_single_etl(
        source_file=APPOINTMENT_SOURCE_FILE,
        output_file=APPOINTMENT_OUTPUT_FILE,
        transform_func=transform_appointment_row,
        fieldnames=[
            "appointment_id",
            "customer_id",
            "employee_id",
            "service_name",
            "appointment_start",
            "appointment_end",
            "duration_minutes",
            "status",
            "service_price",
            "payment_method",
            "tip_amount",
        ],
    )

    run_appointment_enrichment()
    run_daily_summary()


if __name__ == "__main__":
    run_etl()
