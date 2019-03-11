import numpy as np
import pandas as pd
import bz2
import xml.sax
import mwparserfromhell
import os
from time import time
from wiki_xml_handler import WikiXMLHandler
from multiprocessing import Pool

wiki_dump = 'C:/data/enwiki-20190220-pages-articles-multistream1.xml-p10p30302.bz2'
input_folder = 'C:/data/'
output_folder = 'D:/DAT500-Project-Wiki/clean-data/'
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

    if save:
        temp = []
        for page in handler._pages:
            temp.append({
                'id': page[0],
                'title': page[1],
                'text': [page[2]],
                'wikilinks': [page[3]],
                'extlinks': [page[4]]
            })
        csv = pd.DataFrame(temp,
                           columns=['id', 'title', 'text', 'wikilinks', 'extlinks'])
        csv.to_csv(output_folder+data_path+'.csv', sep='\t', index=False)
    
    end = time()
    print(f'\n{data_path} preprocessed in {round(end-start)} seconds')
    print(f'{handler._page_count} pages found in {data_path}')


def main():
    #start = time()
    ## Create a pool of workers to execute processes
    #pool = Pool(processes = 4)
#
    ## Map (service, task), applies function to each partition 
    #pool.map(preprocess_pages, partitions)
#
    #pool.close()
    #pool.join()
    #end = time()
    #print(f'\nWhole dump preprocessed in {round(end-start)} seconds')

    preprocess_pages(partitions[0])


if __name__ == '__main__':
    main()