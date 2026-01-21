from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import zipfile
import io
import os
from pydantic import BaseModel
from typing import Optional


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


class Book(BaseModel):
    ISBN: str
    Book_Title: str
    Book_Author: str
    Year_Of_Publication: Optional[int]
    Publisher: str
    Image_URL_S: Optional[str]
    Image_URL_M: Optional[str]
    Image_URL_L: Optional[str]


@dag(
    dag_id="industrial_etl_pipeline_taskflow",
    description="Extract from multiple sources and upload merged data to MinIO",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "taskflow", "minio"],
)
def industrial_etl_pipeline():
    # -------------------------
    # Extract tasks
    # -------------------------

    @task()
    def extract_from_db() -> list[dict]:
        print("Extracting from DB...")
        return [{"source": "db", "value": 1}]

    @task()
    def extract_from_website() -> list[dict]:
        print("Extracting from website...")
        return [{"source": "web", "value": 2}]

    @task.virtualenv(requirements=["httpx", "pydantic"], system_site_packages=False)
    def extract_from_zip() -> list[dict]:
        import httpx, zipfile, io, csv
        from pydantic import ValidationError
        from typing import List
        from __main__ import Book

        file_id = "1s-x76gQ-eoM5sqT2Hhcfn087Aw5D__hD"
        url = f"https://drive.google.com/uc?export=download&id={file_id}"

        books: List[dict] = []

        # Download ZIP in memory
        try:
            with httpx.Client() as client:
                response = client.get(url)
                response.raise_for_status()
        except Exception as e:
            print(f"Download failed: {e}")
            raise

        # Extract CSV in memory and parse
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            for filename in z.namelist():
                if filename.endswith(".csv"):
                    with z.open(filename) as csvfile:
                        reader = csv.DictReader(
                            io.TextIOWrapper(csvfile, encoding="utf-8")
                        )
                        for row in reader:
                            try:
                                book = Book(**row)
                                books.append(book.dict())  # store dicts for XCom
                            except ValidationError as e:
                                print(f"Skipping invalid row: {e}")

        print(f"Extracted {len(books)} books from ZIP")
        return books

    # -------------------------
    # Merge task
    # -------------------------

    @task
    def merge_data(
        db_data: list[dict],
        web_data: list[dict],
        zip_data: list[dict],
    ) -> str:
        """
        Merge all extracted data and write to a local file.
        Returns the file path (small string → safe for XCom).
        """
        merged = db_data + web_data + zip_data

        output_dir = "/opt/airflow/data"
        os.makedirs(output_dir, exist_ok=True)

        file_path = f"{output_dir}/merged_data_{datetime.utcnow().date()}.json"

        with open(file_path, "w") as f:
            json.dump(merged, f)

        print(f"Merged {len(merged)} records into {file_path}")
        return file_path

    # -------------------------
    # Upload task
    # -------------------------

    @task
    def upload_to_minio(file_path: str) -> None:
        print(f"Uploading {file_path} to MinIO...")

        hook = S3Hook(aws_conn_id="minio_default")

        hook.load_file(
            filename=file_path,
            key=f"etl/merged/{os.path.basename(file_path)}",
            bucket_name="industrial-data",
            replace=True,
        )

    # -------------------------
    # DAG wiring
    # -------------------------

    db_data = extract_from_db()
    web_data = extract_from_website()
    zip_data = extract_from_zip()

    merged_file = merge_data(db_data, web_data, zip_data)
    upload_to_minio(merged_file)


dag = industrial_etl_pipeline()
