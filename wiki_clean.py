import numpy as np
import pandas as pd
import bz2
import xml.sax
import mwparserfromhell
import os
import json
from time import time
from wiki_xml_handler import WikiXMLHandler
from multiprocessing import Pool

wiki_dump = 'C:/data/enwiki-20190220-pages-articles-multistream1.xml-p10p30302.bz2'
data_folder = 'C:/data/'
partitions = [data_folder + file for file in os.listdir(data_folder) if 'xml-p']

def preprocess_pages(data_path, save=True):
    """Finds and cleans all pages from a compressed wikipedia XML file"""
    start = time()
    # Object for handling xml
    handler = WikiXMLHandler()

    # Parsing object
    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)

    # Iteratively process file
    i = 0
    for line in bz2.BZ2File(data_path, 'r'):
        try:
            parser.feed(line)
        except StopIteration:
            break
        i += 1
        if i > 1e+4: break

    if save:
        # Save to a file based on the data path name
        pass
    
    end = time()
    print(f'\n{data_path} preprocessed in {round(end-start)} seconds')
    print(f'{handler._page_count} pages found in {data_path}')
    return handler._pages


def main():
    start = time()
    # Create a pool of workers to execute processes
    pool = Pool(processes = 4)

    # Map (service, task), applies function to each partition 
    results = pool.map(preprocess_pages, partitions)

    pool.close()
    pool.join()
    end = time()
    print(f'\nWhole dump preprocessed in {round(end-start)} seconds')
    print(f'\nLength of results {len(results[-1])}')


if __name__ == '__main__':
    main()