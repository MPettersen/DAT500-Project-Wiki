import xml.sax
import mwparserfromhell
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

class WikiXMLHandler(xml.sax.handler.ContentHandler):
    """Content handler for Wiki XML data using SAX"""
    def __init__(self):
        xml.sax.handler.ContentHandler.__init__(self)
        self._buffer = None
        self._values = {}
        self._current_tag = None
        self._previous_tag = None
        self._pages = []
        self._punctuations = set(['.', ',', ';', ':', '?', '!', '#', '\\', '/', '"', '\'', '\'\'', '´´', '´', '``', '`', '(', ')'])
        self._stop_words = set(stopwords.words('english'))
        self._filter = self._punctuations.union(self._stop_words)
        
        
    def characters(self, content):
        """Characters between opening and closing tags"""
        if self._current_tag:
            self._buffer.append(content)
            
            
    def startElement(self, name, attrs):
        """Opening tag of element"""
        if name in ('id', 'title', 'text'):
            self._previous_tag = self._current_tag
            self._current_tag = name
            self._buffer = []
            
        
    def endElement(self, name):
        """Closing tag of element"""
        if name == self._current_tag:
            if name == 'text':
                if self._redirect():
                    self._skip_page = True
                    pass
                else:
                    self._skip_page = False
                self._process_page()
            elif name == 'id' and self._previous_tag == 'id':
                pass
            else:
                self._values[name] = ' '.join(self._buffer)
        if name == 'page':
            if not self._skip_page:
                self._pages.append((self._values['id'],
                                    self._values['title'],
                                    self._values['text'],
                                    self._values['wikilinks']))
                self._page_count = len(self._pages)
                print(f'Number of pages processed: {self._page_count}, Current page: {self._pages[-1][0]}')
    
    
    def _redirect(self):
        """Checking if the page is a redirect page or not"""
        wiki = mwparserfromhell.parse(self._buffer)
        text = wiki.strip_code().split()
        if len(text) == 0:
            return True
        return text[0] == 'REDIRECT'
    
    
    def _process_page(self):
        """
        Processing the text and retrieving the internal wikilinks and the
        external url-links.
        """
        content = mwparserfromhell.parse(
            mwparserfromhell
            .parse(self._buffer)
            .strip_code()
            .strip())
        self._values['text'] = list(filter(
            lambda word: word not in filters,
            word_tokenize(content.strip_code().strip())))
        self._values['wikilinks'] = [x.title.strip_code() for x in content.filter_wikilinks()]