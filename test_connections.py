"""Quick connectivity test for Databricks and OpenAI. Run before anything else."""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

def test_databricks():
    print("Testing Databricks connection...")
    required = ["DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_ACCESS_TOKEN"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        print(f"  FAIL — missing env vars: {', '.join(missing)}")
        return False

    try:
        from databricks import sql
        conn = sql.connect(
            server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            access_token=os.environ["DATABRICKS_ACCESS_TOKEN"],
        )
        cursor = conn.cursor()
        cursor.execute("SELECT current_user(), current_timestamp()")
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        print(f"  OK — connected as {row[0]} at {row[1]}")
        return True
    except Exception as e:
        print(f"  FAIL — {e}")
        return False


def test_databricks_tables():
    print("Checking expected Delta tables exist...")
    expected = [
        "nsta_field_production_raw",
        "nsta_operator_mapping",
        "nsta_news_raw",
        "rns_announcements_raw",
        "company_briefings",
    ]
    try:
        from databricks import sql
        conn = sql.connect(
            server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            access_token=os.environ["DATABRICKS_ACCESS_TOKEN"],
        )
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        existing = {row[1].lower() for row in cursor.fetchall()}
        cursor.close()
        conn.close()

        for table in expected:
            status = "OK" if table in existing else "MISSING"
            print(f"  {status} — {table}")
        return True
    except Exception as e:
        print(f"  FAIL — {e}")
        return False


def test_openai():
    print("Testing OpenAI connection...")
    if not os.environ.get("OPENAI_API_KEY"):
        print("  FAIL — OPENAI_API_KEY not set")
        return False

    try:
        from openai import OpenAI
        client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": "Reply with the single word: ok"}],
            max_tokens=5,
        )
        reply = response.choices[0].message.content.strip()
        print(f"  OK — gpt-4o responded: '{reply}'")
        return True
    except Exception as e:
        print(f"  FAIL — {e}")
        return False


if __name__ == "__main__":
    results = []
    db_ok = test_databricks()
    results.append(db_ok)
    print()
    if db_ok:
        results.append(test_databricks_tables())
    else:
        print("Skipping table check — fix connection first.")
        results.append(False)
    print()
    results.append(test_openai())
    print()

    if all(results):
        print("All connections OK.")
    else:
        print("One or more connections failed — check errors above.")
        sys.exit(1)
