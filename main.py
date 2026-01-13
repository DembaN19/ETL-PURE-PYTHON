from datetime import datetime
import logging as logger
from src.utils import *
from src.setup import *

def main():
    start_timestamp = datetime.now()
    logger.info(f'Start timestamp: {datetime.now()}')


    # Time duration
    end_timestamp = datetime.now()
    logger.info(f'Traitement finished timestamp: {end_timestamp}')
    traitement_duration = diff_time(start_timestamp , end_timestamp)
    logger.info(f'Duration job: {traitement_duration[0]}h:{traitement_duration[1]}m:{traitement_duration[2]}s ')

if __name__ == "__main__":
    main()
    