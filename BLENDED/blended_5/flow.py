import os
import subprocess
from dotenv import load_dotenv
from prefect import flow, task
from BLENDED.blended_5.data_generation import generate_day1_data, generate_day2_changes

load_dotenv()

DAY_INIT = os.getenv("DAY_INIT", "BLENDED/blended_5/data/day1_customers.csv")
DAY_CHANGE = os.getenv("DAY_CHANGE", "BLENDED/blended_5/data/day2_changes.csv")


@task
def generate_day1(num_records=100):
    generate_day1_data(num_records=num_records, output_file=DAY_INIT)


@task
def generate_day2(num_records=100, percent_change=0.2):
    generate_day2_changes(num_records=num_records, percent_change=percent_change, day1_file=DAY_INIT, day2_file=DAY_CHANGE)


def run_subprocess(command):
    """Run a subprocess with .env variables explicitly passed."""
    env = os.environ.copy()  # Копіюємо всі змінні середовища
    try:
        print(f"Running command: {' '.join(command)}")
        subprocess.run(command, check=True, env=env)
        print(f"Command succeeded: {' '.join(command)}")
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {' '.join(command)}")
        print(f"Return code: {e.returncode}")
        raise
    except Exception as e:
        print(f"Unexpected error while executing command: {' '.join(command)}")
        raise


# ===== SCD2 FLOWS =====
@task
def load_day1_scd2():
    run_subprocess(["python", "BLENDED/blended_5/postgres_type_2_load_day1.py"])
    run_subprocess(["python", "BLENDED/blended_5/couchdb_type_2_load_day1.py"])
    run_subprocess(["python", "BLENDED/blended_5/neo4j_type_2_load_day1.py"])


@task
def load_day2_scd2():
    run_subprocess(["python", "BLENDED/blended_5/postgres_type_2_load_day2.py"])
    run_subprocess(["python", "BLENDED/blended_5/couchdb_type_2_load_day2.py"])
    run_subprocess(["python", "BLENDED/blended_5/neo4j_type_2_load_day2.py"])


@flow(name="scd2_day1_flow")
def scd2_day1_flow(num_records=100):
    generate_day1(num_records)
    load_day1_scd2()


@flow(name="scd2_day2_flow")
def scd2_day2_flow(num_records=100, percent_change=0.2):
    generate_day2(num_records, percent_change)
    load_day2_scd2()


# ===== SCD3 FLOWS =====
@task
def load_day1_scd3():
    run_subprocess(["python", "BLENDED/blended_5/postgres_type_3_load_day1.py"])
    run_subprocess(["python", "BLENDED/blended_5/couchdb_type_3_load_day1.py"])
    run_subprocess(["python", "BLENDED/blended_5/neo4j_type_3_load_day1.py"])


@task
def load_day2_scd3():
    run_subprocess(["python", "BLENDED/blended_5/postgres_type_3_load_day2.py"])
    run_subprocess(["python", "BLENDED/blended_5/couchdb_type_3_load_day2.py"])
    run_subprocess(["python", "BLENDED/blended_5/neo4j_type_3_load_day2.py"])


@flow(name="scd3_day1_flow")
def scd3_day1_flow(num_records=100):
    generate_day1(num_records)
    load_day1_scd3()


@flow(name="scd3_day2_flow")
def scd3_day2_flow(num_records=100, percent_change=0.2):
    generate_day2(num_records, percent_change)
    load_day2_scd3()


# ===== SCD4 FLOWS =====
@task
def load_day1_scd4():
    run_subprocess(["python", "BLENDED/blended_5/postgres_type_4_load_day1.py"])
    run_subprocess(["python", "BLENDED/blended_5/couchdb_type_4_load_day1.py"])
    run_subprocess(["python", "BLENDED/blended_5/neo4j_type_4_load_day1.py"])


@task
def load_day2_scd4():
    run_subprocess(["python", "BLENDED/blended_5/postgres_type_4_load_day2.py"])
    run_subprocess(["python", "BLENDED/blended_5/couchdb_type_4_load_day2.py"])
    run_subprocess(["python", "BLENDED/blended_5/neo4j_type_4_load_day2.py"])


@flow(name="scd4_day1_flow")
def scd4_day1_flow(num_records=100):
    generate_day1(num_records)
    load_day1_scd4()


@flow(name="scd4_day2_flow")
def scd4_day2_flow(num_records=100, percent_change=0.2):
    generate_day2(num_records, percent_change)
    load_day2_scd4()

    
    
import argparse

if __name__ == "__main__":
    # Define argument parser
    parser = argparse.ArgumentParser(description="Choose which flow to run.")
    parser.add_argument(
        "flow",
        choices=["scd2-day1", "scd2-day2", "scd3-day1", "scd3-day2", "scd4-day1", "scd4-day2"],
        help="Specify which flow to execute."
    )

    args = parser.parse_args()

    # Map flow names to their respective objects
    flow_map = {
        "scd2-day1": scd2_day1_flow,
        "scd2-day2": scd2_day2_flow,
        "scd3-day1": scd3_day1_flow,
        "scd3-day2": scd3_day2_flow,
        "scd4-day1": scd4_day1_flow,
        "scd4-day2": scd4_day2_flow,
    }

    selected_flow = flow_map[args.flow]

    # Run the selected flow
    print(f"Running flow: {args.flow}")
    selected_flow()

    print(f"Flow '{args.flow}' completed.")

