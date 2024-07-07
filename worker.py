from tasks import app
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="TPG Worker")
    parser.add_argument("--worker-name", type=str, help="Name of the worker")
    args = parser.parse_args()
    
    if not args.worker_name:
        parser.error("Please provide --worker-name")

    app.worker_main(['worker', '--loglevel=info', '--concurrency=1', "-n", args.worker_name, "-Q", args.worker_name])
    
