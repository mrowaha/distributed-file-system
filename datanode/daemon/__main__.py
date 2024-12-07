from pathlib import Path
import logging
import subprocess
import os
import argparse

# Get the current working directory
daemon_dir = Path.cwd()
daemon_execution=["go", "run", "."]

def run_loading_datanode(*, dbFile: str, service: str):
    env = os.environ.copy()
    env["DBFile"] = dbFile
    env["Service_Port"]=service

    exitCode = 1
    while exitCode != 0:
        try:
            process = subprocess.Popen(
                daemon_execution,
                stdout=subprocess.PIPE,  # Capture standard output
                stderr=subprocess.PIPE,   # Capture standard error
                env=env,
                text=True
            )

            while True:
                if process.poll() is not None:
                    break

                output = process.stdout.readline()
                logging.info(f"[daemon out] {output}")
                error = process.stderr.readline()
                logging.error(f"[daemon error] {error}")
            exitCode = process.returncode

        except Exception as e:
            print(f"An error occurred in daemon: {e}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-db", "--dbfile", help="name of the database file", required=True)
    ap.add_argument("--service", help="address of the exposed service to send chunks", required=True)
    args = vars(ap.parse_args())
    logging.info(f"daemon dir: {daemon_dir}")
    run_loading_datanode(dbFile=args["dbfile"], service=args["service"])