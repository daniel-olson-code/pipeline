import multiprocessing
import contextlib
import time

try:
    from c_worker import main as run_worker
    from c_bucket import main as run_bucket
    from c_pipeline import main as run_pipeline
except ModuleNotFoundError:
    from worker import main as run_worker
    from bucket import main as run_bucket
    from pipeline import main as run_pipeline


def attempted_to_kill(process: multiprocessing.Process):
    """
    Attempts to kill a process gracefully

    Args:
        process (multiprocessing.Process): The process to kill

    Returns:
        None
    """
    with contextlib.suppress(Exception):
        process.terminate()
        process.kill()


def main():
    """
    Main function to run the pipeline

    Returns:
        None
    """
    # open bucket first to allow pipeline and worker to stor files
    bucket_process = multiprocessing.Process(target=run_bucket)
    # then open pipeline to allow worker to request steps
    pipeline_process = multiprocessing.Process(target=run_pipeline)
    # the start as many workers as you would like
    worker1_process = multiprocessing.Process(target=run_worker)
    worker2_process = multiprocessing.Process(target=run_worker)
    worker3_process = multiprocessing.Process(target=run_worker)

    bucket_process.start()
    pipeline_process.start()
    time.sleep(1)
    worker1_process.start()
    worker2_process.start()
    worker3_process.start()

    try:
        bucket_process.join()
        pipeline_process.join()
        worker1_process.join()
        worker2_process.join()
        worker3_process.join()
    finally:
        print("Clean up")
        attempted_to_kill(worker1_process)
        attempted_to_kill(worker2_process)
        attempted_to_kill(worker3_process)
        attempted_to_kill(pipeline_process)
        attempted_to_kill(bucket_process)


if __name__ == "__main__":  # confirms that the code is under main function
    main()





