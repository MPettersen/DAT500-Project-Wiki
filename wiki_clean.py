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

wiki_dump = 'D:/data/enwiki-20190220-pages-articles-multistream1.xml-p10p30302.bz2'
input_folder = 'data/'
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
    for line in bz2.BZ2File(input_folder+data_path, 'r'):
        try:
            parser.feed(line)
        except StopIteration:
            break
    print(f'Done with the preprocessing of {data_path}')
    if save:
        with open(output_folder+data_path+'.csv', 'w', encoding='utf-8') as csvFile:
            writer = csv.writer(csvFile)
            for i, page in enumerate(handler._pages):
                temp = []
                for j, item in enumerate(page):
                    if j == 2:
                        temp.append(4)
                        temp.extend(item)
                    elif j == 3:
                        temp.insert(3, len(item))
                        temp.extend(item)
                    else:
                        temp.append(item)
                writer.writerow(temp)
        csvFile.close() 
    
    end = time()
    print(f'\n{data_path} preprocessed in {round(end-start)} seconds')
    print(f'{handler._page_count} pages found in {data_path}')


def main():
    start = time()
    # Create a pool of workers to execute processes
    pool = Pool(processes = 6)

    # Map (service, task), applies function to each partition
    pool.map(preprocess_pages, partitions)

    pool.close()
    pool.join()
    end = time()
    print(f'\nWhole dump preprocessed in {round(end-start)} seconds')

    #preprocess_pages(partitions[0])


if __name__ == '__main__':
    main()