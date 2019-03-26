from mrjob.job import MRJob
import random

class MRMultilineInput(MRJob):               
    def mapper_init(self):
        self.article_id = ''
		self.Title = ""
        self.in_text = False
        self.text = []
		self.in_links = False'
		self_links = []
    
    def mapper(self, _, line):
        line = line.strip()
        if line.find('<title>') != -1:
			self.Title = line[line.find('>') + 1: line.find('<')]
        if line.find('<id>') != -1 and not self.article_id:
			self.article_id = line[line.find('>') + 1: line.find('<')]
			
        if not line and not self.in_body:
            self.in_body=True
        
        if line.find('From general') == 0 and self.in_body:
            yield self.message_id, ''.join(self.body)
            self.message_id = ''
            self.body = []    
            self.in_body = False
        
        if self.in_body:
            self.body.append(line)
                       
if __name__ == '__main__':
    MRMultilineInput.run()