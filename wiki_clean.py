import xml.sax
import subprocess as sp
from wiki_xml_handler import WikiXMLHandler

wiki = 'D:/DAT500 project/enwiki-20190101-pages-articles-multistream.xml.bz2'
handler = WikiXMLHandler()
parser = xml.sax.make_parser()
parser.setContentHandler(handler)

def main():
    for line in sp.Popen(['bzcat'], 
                         stdin=open(wiki),
                         stdout=sp.PIPE).stdout:
        parser.feed(line)
        if len(handler.pages) > 2:
            break
    
    print(handler.pages)


if __name__ == '__main__':
    main()