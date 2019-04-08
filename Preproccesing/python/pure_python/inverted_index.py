from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONProtocol

class MRInvertedIndex(MRJob):
    INPUT_PROTOCOL = JSONProtocol

    def mapper(self, article_id, ngrams):
        for ngram in ngrams:
            if article_id:
                yield ngram, article_id
    

    def combiner(self, ngram, article_ids):
        article_ids_set = set(article_ids)
        for article_id in article_ids_set:
            yield ngram, article_id
    

    def reducer(self, ngram, article_ids):
        article_ids_list = list(set(article_ids))
        yield ngram, article_ids_list


if __name__ == '__main__':
    MRInvertedIndex.run()