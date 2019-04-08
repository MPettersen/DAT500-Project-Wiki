from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONProtocol
from math import log


## Need to find:
# N = Total terms in an article
# n = count of a specific term in an article
# D = total number of articles
# d = number of documents where a specific term occurs



class MR_TFIDF(MRJob):
    INPUT_PROTOCOL = JSONProtocol
    
    def mapper_get_n_init (self):
        self.article_count = 0
    
    
    # article_id, article_info -> (article_id,term,N), count
    def mapper_get_n(self, article_id, article_info):
        self.article_count += 1
        article_info = list(article_info)
        terms = list(article_info[0])
        N = len(terms)
        for term in terms:
            yield (article_id,term.lower(),N), 1
            
    def mapper_get_n_final (self):
        yield 'article_count', self.article_count

    def reducer_get_n_init (self):
        self.keys = []
        self.value = []
        self.article_count = 0
        
    # (article_id,term,N), count  ->  (article_id,term,N,D), sum(count) = n
    def reducer_get_n (self, term_info, count):
        if term_info == 'article_count':
            self.article_count += sum(count)
        else:
            self.keys.append((term_info[0],term_info[1], term_info[2]))
            self.value.append(sum(count))

    def reducer_get_n_final (self):
        for i in range (len(self.value)):
            yield (self.keys[i][0],self.keys[i][1],self.keys[i][2],self.article_count), self.value[i]
            
            
    #(article_id,term,N,D), n -> term, (article_id,n,N,D)
    def mapper_get_d (self, term_info, n):
        yield term_info[1], (term_info[0],n,term_info[2],term_info[3])
        
    def reducer_get_d_init(self):
        self.value = []
        self.d = 0
        self.D = 0
        
    #term, (article_id,n,N,D) -> 
    def reducer_get_d (self,term,term_info):
        for t in term_info:
            if self.D == 0 and t[3]:
                self.D = t[3]
            self.d += 1
            self.value.append((t[0],t[1],t[2],t[3]))
        for v in self.value:
            yield term, (v[0],v[1],v[2],self.d,self.D)
        self.value = []
        self.d = 0

            
    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_get_n_init,
                   mapper = self.mapper_get_n,
                   mapper_final = self.mapper_get_n_final,
                   reducer_init =self.reducer_get_n_init,
                   reducer = self.reducer_get_n,
                   reducer_final = self.reducer_get_n_final),
            MRStep( mapper=self.mapper_get_d,
                  reducer_init = self.reducer_get_d_init,
                  reducer=self.reducer_get_d)

               ]



if __name__ == '__main__':
    MR_TFIDF.run()