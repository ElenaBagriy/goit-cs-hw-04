from pathlib import Path
from threading import Thread
from multiprocessing import Process, Queue
from time import perf_counter


# Define the directory containing the text files
folder = Path("texts")
# Define the keywords to search for
keywords = ["Python", "threading", "error"]
# Define the number of threads and processes
NUM_WORKERS = 2


def process_multiproc_worker(group, keywords, result_queue):
    result = search_keywords_in_files(group, keywords)
    result_queue.put(result)

def search_keywords_in_files(file_paths, keywords):
    """Search for keywords in the contents of the given files."""
    # Initialize a dictionary with an empty list for each keyword
    results = {keyword: [] for keyword in keywords}
    for file_path in file_paths:
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                text = file.read()
            # Check whether each keyword occurs in the file
            for word in keywords:
                if word.lower() in text.lower():
                    # Store the path if the keyword is found
                    results[word].append(str(file_path))
        except OSError as e:
            # Handle file-related errors, such as missing files or permission issues
            print(f"Error reading {file_path}: {e}")
    return results


def split_files(files, workers):
    """Divide the list of files into groups for parallel processing."""
    # Ensure that the number of workers is positive
    if workers <= 0:
        raise ValueError("The number of workers must be greater than zero")
    # Initialize a list to store the file groups
    file_groups = []
    # Calculate the base number of files per group
    chunk_size = len(files) // workers
    # Calculate how many files remain after an even division
    remainder = len(files) % workers
    # Track the starting index of the next group
    start = 0
    # Create one group for each worker
    for i in range(workers):
        group_size = chunk_size
        # Distribute the remaining files among the first groups
        if i < remainder:
            group_size += 1
        # Calculate the ending index of the current group
        end = start + group_size
        # Add the current group to the list
        file_groups.append(files[start:end])
        # Move the starting index to the next group
        start = end
    return file_groups


def search_with_threads(file_paths, keywords, number_of_workers):
    """Search files using multiple threads."""
    # Divide the files into groups
    file_groups = split_files(file_paths, number_of_workers)
    thread_results = [None] * len(file_groups)
    threads = []
    def worker(index, group):
        thread_results[index] = search_keywords_in_files(group, keywords)
    for i, group in enumerate(file_groups):
        thread = Thread(target=worker, args=(i, group))
        threads.append(thread)
    # Start all threads
    for thread in threads:
        thread.start()
    # Wait for all threads to finish
    for thread in threads:
        thread.join()
    # Initialize the combined results dictionary
    results = {keyword: [] for keyword in keywords}
    # Merge the results from all threads
    for thread_result in thread_results:
        for keyword, paths in thread_result.items():
            results[keyword].extend(paths)

    return results


def search_with_multiprocessing(file_paths, keywords, number_of_workers):
    """Search files using multiple processes."""
    # Divide the files into groups
    file_groups = split_files(file_paths, number_of_workers)
    # Create a queue for collecting results
    result_queue = Queue()
    processes = []
    # Create a process for each file group
    for group in file_groups:
        process = Process(
            target=process_multiproc_worker,
            args=(group, keywords, result_queue)
        )
        processes.append(process)
    # Start all processes
    for process in processes:
        process.start()
    # Collect results from all processes
    process_results = []
    for _ in processes:
        process_results.append(result_queue.get())
    # Wait for all processes to finish
    for process in processes:
        process.join()
    # Initialize the combined results dictionary
    results = {keyword: [] for keyword in keywords}
    # Merge the results from all processes
    for process_result in process_results:
        for keyword, paths in process_result.items():
            results[keyword].extend(paths)
    result_queue.close()
    return results


def measure_time(search_function, file_paths, keywords, number_of_workers):
    """Measure the execution time of a search function."""
    # Record the start time
    start_time = perf_counter()
    # Execute the selected search function and collect its results
    results = search_function(file_paths, keywords, number_of_workers)
    # Record the end time
    end_time = perf_counter()
    elapsed_time = end_time - start_time
    return results, elapsed_time


def main():
    # Find all text files in the specified directory
    file_paths = list(folder.glob("*.txt"))
    # Stop execution if no text files are found
    if not file_paths:
        print("No text files found")
        return
    results_threading = measure_time(search_with_threads, file_paths, keywords, NUM_WORKERS)
    results_multiproc = measure_time(search_with_multiprocessing, file_paths, keywords, NUM_WORKERS)
    print(f"Results found with threading:\n{results_threading[0]}")
    print(f"Time spent: {results_threading[1]:.6f}\n")
    print(f"Results found with multiprocessing:\n{results_multiproc[0]}")
    print(f"Time spent: {results_multiproc[1]:.6f}")


if __name__ == "__main__":
    main()