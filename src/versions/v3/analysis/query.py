"""Query module."""

from concurrent import futures
from itertools import product

from database.databaseconnection import DataBase
from utilites.utility import hash_code
from utilites.utility import normalization


class QueryAnalysis:
    def __init__(self):
        self.database = DataBase()

    def lemma_relation(self, lemma):
        output = [self.database.query_relation(normalization(i[0])) for i in lemma if i != []]
        output = [i for i in output if i != []]
        if not output:
            output = [self.database.query_relation_lemma(i[1]) for i in lemma if i != []]
        return output

    def query_relation(self, lemma, ne):
        with futures.ThreadPoolExecutor(3) as executor:
            task1 = executor.submit(self.lemma_relation, lemma)
            task2 = executor.submit(self.ne_to_id, ne)
        lemma = [i[0] for i in task1.result() if i != []]

        ne = task2.result()

        product_ = list(product(lemma, ne))

        with futures.ThreadPoolExecutor(3) as executor:
            output = [executor.submit(self.database.query_find_relation, i[1], i[0]).result() for i in product_]

        output = [i for i in output if i != []]
        output = [ii[0] for i in output for ii in i]
        return output

    def ne_to_id(self, nes):
        uid = [self.database.get_word_id(hash_code(normalization(id_))) for id_ in nes]
        return [ii for i in uid for ii in i]
