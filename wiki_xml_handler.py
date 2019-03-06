import xml.sax
import subprocess

class WikiXMLHandler(xml.sax.handler.ContentHandler):
    """Content hadler for Wiki XML data using SAX"""
    def __init__(self):
        xml.sax.handler.ContentHandler.__init__(self)
        self.__buffer = None
        self.__values = {}
        self.__current_tag = None
        self.pages = []


    def __characters(self, content):
        """Characters between opening and closing tags"""
        if self.__current_tag:
            self.__buffer.append(content)


    def __startElement(self, name):
        """Opening tag of element"""
        if name in ('title', 'text'):
            self.__current_tag = name
            self.__buffer = []
    

    def __endElement(self, name):
        """Closing tag of element"""
        if name == self.__current_tag:
            self.__values[name] = ' '.join(self.__buffer)
        if name == 'page':
            self.pages.append((self.__values['title'], self.__values['text']))