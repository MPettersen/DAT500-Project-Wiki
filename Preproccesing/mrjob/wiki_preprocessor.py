from mrjob.job import MRJob
import mwparserfromhell as mwp
from nltk.corpus import stopwords
import re
import nltk
import json

class MR_wikiprocessor(MRJob):      

    def mapper_init(self):
        self.in_article = False
        self.article_id = ''
        self.Title = ""
        self.in_text = False
        self.text = []
        self.in_links = False
        self.links = []
        self.titles_to_ignore = ['Wikipedia:', 'File:', 'Template:']
        
    def return_tokens_wo_stopwords (self,s):
        punctuations = set(['.', ',', ';', ':', '?', '!', '#', '\\', '/', '"', '\'', '\'\'', '´´', '´', '``', '`', '(', ')'])
        stop_words = set(stopwords.words('english'))
        filter = punctuations & stop_words
        s = re.sub(r'([^\s\w]|_)+', ' ', s)
        tokens = s.split()
        remove = set(tokens) &  stop_words
        tokens = [x for x in tokens if x.lower() not in remove]
        return tokens

    def return_links (self,tl):
        link_list = []
        for l in tl:
            link_list.append(str(l.title))
        return link_list

    def handle_text_string (self,s):
        s = mwp.parse(s)
        links = s.filter_wikilinks()
        links = self.return_links(links)
        s = s.strip_code()
        s = self.return_tokens_wo_stopwords(s)
        return s,links
    
    def mapper(self, _, line):
    
        line = line.strip()
        if self.in_article:
            if self.in_text:
                if line.find('/text') != -1:
                    self.in_text = False
                    s = line[0:line.find('<')]
                    s,l = self.handle_text_string(s)
                else:
                    s,l = self.handle_text_string(line)
                self.text.extend(s)
                self.links.extend(l)
            elif line.find('</page>') != -1 and self.in_article:
                self.in_article = False
                article_info = []
                article_info.append(self.title)
                article_info.append(self.text)
                article_info.append(self.links)
                self.title = ""
                temp_id = self.article_id
                self.article_id = ""          
                self.text = []
                self.links = []
                yield temp_id, article_info
            elif line.find('<text') != -1:
                if line.find('#REDIRECT') != -1:
                        self.in_article = False
                        self.title = ""
                        self.id = ""
                else:
                    self.in_text = True               
            elif line.find('<title>') != -1 and not self.Title:
                if any(x in line for x in self.titles_to_ignore):
                    self.in_article = False
                else:
                    i = line.find('>') + 1
                    self.title = line[i: line.find('<', i)]
            elif line.find('<id>') != -1 and not self.article_id:
                i = line.find('>') + 1
                self.article_id = line[i: line.find('<', i)]
        elif line.find('<page>') != -1:
            self.in_article = True
            
                       
if __name__ == '__main__':
    MR_wikiprocessor.run()