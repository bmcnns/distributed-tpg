from tasks import start_simulator_runners
from datetime import datetime
import pandas as pd

import uuid

if __name__ == '__main__':
    worker_batch_sizes = {
        'desktop': 1,
    }

    teams_per_worker = {
        'desktop':  [str(uuid.uuid4()) for _ in range(36)]
    }

    num_generations = 100
    
    time_elapsed = 0 
    data = []
    
    for generation in range(num_generations):
        print(f"Starting generation {generation}...")
        
        start_time = datetime.now()

        start_simulator_runners(
            worker_batch_sizes,
            teams_per_worker,
            generation=generation,
        )

        print(f"Finished generation {generation}...")

        end_time = datetime.now()

        dt = end_time - start_time
        
        time_elapsed += dt.total_seconds()

        data.append( [generation,  time_elapsed] )

    benchmarking_data = pd.DataFrame(data, columns=['Generation', 'Time Elapsed'])

    print(benchmarking_data.head())
    benchmarking_data.to_csv("generation_data/distributed_benchmarking_by_generation.csv")
