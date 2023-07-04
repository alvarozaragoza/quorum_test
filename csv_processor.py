"""
Name :        csv_processor
Author :      Alvaro Zaragoza
Last Update : July, 04, 2023
Description : Read CSV files, calculate votes, and save results in a new CSV File 
"""

import logging
import sys
import platform
from datetime import datetime
import pandas

# Create a looger function
def my_custom_logger(logger_name, level=logging.DEBUG):
    """
    Method to return a custom logger with the given name and level
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    format_string = "%(asctime)s — line: %(lineno)d — %(message)s"
    log_format = logging.Formatter(format_string)
    # Creating and adding the console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)
    # Creating and adding the file handler
    file_handler = logging.FileHandler(logger_name, mode='a')
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)
    return logger

# Create main function
def main():
    # Folder separator accordind to OS
    folder_sep = "\\" if platform.system() == "Windows" else "/"

    # Get current time as string
    now = datetime.now()
    current_time = now.strftime("%m_%d_%Y-%H_%M_%S")

    # # Create new log file
    logger = my_custom_logger(f'log{folder_sep}CSV_Processor_Log_{current_time}.log')

    logger.info("Starting Quorum CSV Processor (version: 1.01)")
    logger.info("---------------------------------------------")

    # Read CSV files into Pandas DataFrames
    logger.info("Reading CSV files...")

    try:
        df_legislators = pandas.read_csv(f'inbox{folder_sep}legislators.csv')
    except:
        logger.info(f'File inbox{folder_sep}legislators.csv not found. App cannot continue.')
        exit()

    try:
        df_bills = pandas.read_csv(f'inbox{folder_sep}bills.csv')
    except:
        logger.info(f'File inbox{folder_sep}bills.csv not found. App cannot continue.')
        exit()

    try:
        df_votes = pandas.read_csv(f'inbox{folder_sep}votes.csv')
    except:
        logger.info(f'File inbox{folder_sep}votes.csv not found. App cannot continue.')
        exit()

    try:
        df_vote_results = pandas.read_csv(f'inbox{folder_sep}vote_results.csv')
    except:
        logger.info(f'File inbox{folder_sep}vote_results.csv not found. App cannot continue.')
        exit()

    logger.info("Legislators, bills, votes, and vote_results imported into Dataframes.")

    # Analyze DataFrames - TO BE DONE
    # Imported data should be analyzed and cleaned

    # Calculate votes and create new Dataframes for results
    logger.info("Calculating votes and grouping by legislators...")

    # Create Dataframes counting votes grouping by legislator_id and vote_type
    df_support = df_vote_results[df_vote_results['vote_type'] == 1]
    df_oppose = df_vote_results[df_vote_results['vote_type'] == 2]
    df_legislator_support = df_support.groupby(['legislator_id']).size().reset_index(name='num_supported_bills')
    df_legislator_oppose = df_oppose.groupby(['legislator_id']).size().reset_index(name='num_opposed_bills')
    
    # Rename columns and merge Dataframes
    df_legislator_support = df_legislator_support.rename(columns={'legislator_id': 'id'})
    df_legislator_oppose = df_legislator_oppose.rename(columns={'legislator_id': 'id'})
    df_legislators_support_oppose_count = pandas.merge( df_legislators, df_legislator_support, how="left",  on="id")
    df_legislators_support_oppose_count = pandas.merge( df_legislators_support_oppose_count, df_legislator_oppose, how="left", on="id")

    # Replace NaN with Zeros
    df_legislators_support_oppose_count.fillna(0,inplace=True)
    
    # Set counting columns as integer
    df_legislators_support_oppose_count['num_supported_bills'] = df_legislators_support_oppose_count['num_supported_bills'].astype('int')
    df_legislators_support_oppose_count['num_opposed_bills'] = df_legislators_support_oppose_count['num_opposed_bills'].astype('int')

    print(df_legislators_support_oppose_count)
    
    # Create Result CSV file

if __name__ == "__main__":
    main()








