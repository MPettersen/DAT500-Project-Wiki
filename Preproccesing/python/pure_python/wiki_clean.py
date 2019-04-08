import numpy as np
import pandas as pd
import bz2
import xml.sax
import mwparserfromhell
import os
import csv
from time import time
from wiki_xml_handler import WikiXMLHandler
from multiprocessing import Pool
from copy import deepcopy as dc

input_folder = 'C:/data/'
#input_folder = 'data/'
output_folder = 'clean-data/'
partitions = [file for file in os.listdir(input_folder) if 'xml-p']

def preprocess_pages(data_path, save=True):
    """Finds and cleans all pages from a compressed wikipedia XML file"""
    start = time()
    # Object for handling xml
    handler = WikiXMLHandler()

    # Parsing object
    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)

    # Iteratively process file
    file = bz2.BZ2File(input_folder+data_path, 'r')
    for line in file:
        try:
            parser.feed(line)
        except StopIteration:
            break
    print(f'\nDone processing {data_path}, now writing csv-file')
    if save:
        with open(output_folder+data_path+'.csv', 'w', encoding='utf-8', newline='') as csvFile:
            writer = csv.writer(csvFile, delimiter='\t')
            for page in handler._pages:
                temp = []
                for j, item in enumerate(page):
                    if j == 2:
                        temp.append(4)
                        temp.extend(dc(item))
                    elif j == 3:
                        temp.insert(3, len(dc(item)))
                        temp.extend(dc(item))
                    else:
                        temp.append(dc(item))
                if len(temp) > 0:
                    writer.writerow(dc(temp))
        csvFile.close()
    end = time()
    print(f'{data_path} preprocessed in {round(end-start)} seconds')
    print(f'{handler._page_count} pages found in {data_path}')
    file.close()


def main():
    start = time()
    
    
    # Create a pool of workers to execute processes
    # IMPORTANT!!!
    #   It is vital to choose the number of processes carefully. Each process
    #   can use in excess of 5GB RAM. Use these estimates if you are unsure:
    #       4GB available: DO NOT RUN
    #       8GB available: 1 process
    #       16GB available: 2 processes
    #       32GB available: 5 processes
    pool = Pool(processes = 5)

    # Map (service, task), applies function to each partition
    pool.map(preprocess_pages, partitions)

    pool.close()
    pool.join()

    #preprocess_pages('wiki_test.bz2')
    end = time()
    print(f'\nWhole dump preprocessed in {round(end-start)} seconds')    


if __name__ == '__main__':
    main()