import decimal
import re

import pyodbc
import xxhash


class ConnectionString:
    def __init__(self):
        self.con = pyodbc.connect(
            'Driver={ODBC Driver 17 for SQL Server};' 'Server=10.107.158.158;' 'Database=Ontology_KB;'
            'UID=SA;' 'PWD=EGY@2020;' "MultipleActiveResultSets=true")


class Term:
    def __init__(self):
        self._term_index_list = []

    @staticmethod
    def normalization(word):
        word = re.sub("[إأٱآا]", "ا", word)
        word = re.sub("ى", "ي", word)
        word = re.sub("ة", "ه", word)
        return word

    @staticmethod
    def get_hash_code(word):
        word_hash_code = decimal.Decimal(xxhash.xxh64(word).intdigest())
        return word_hash_code

    @staticmethod
    def get_term_universal_ids(term_hash_code):
        connection = ConnectionString()
        cursor = connection.con.cursor()
        sql = "select Universal_ID from dbo.OntologyTerm where HashCode_Normalized = ?"
        val = term_hash_code
        cursor.execute(sql, val)
        return cursor.fetchall()

    @staticmethod
    def get_term_index_list(term_uid_list, word_normalized, term_word):
        connection = ConnectionString()
        cursor = connection.con.cursor()
        term_index_dict = {'token': term_word, 'type': 'Term', 'syn': []}
        synonyms_dict = {'Word': []}

        for term_uid in term_uid_list:
            sql = "select distinct TermName from dbo.TermIndex where UID = ? and TermName != ?"
            val = (term_uid[0], term_word)
            cursor.execute(sql, val)
            term_index_words = cursor.fetchall()
            if term_index_words:
                for term_Word in term_index_words:
                    if term_Word[0] not in synonyms_dict['Word'] and Term.normalization(
                            term_Word[0]) != word_normalized:
                        synonyms_dict['Word'].append(term_Word[0])
            sql = "select Term_Original from dbo.OntologyTerm where Universal_ID = ? and Term_Normalized != ?"
            val = (term_uid[0], word_normalized)
            cursor.execute(sql, val)
            term_words = cursor.fetchall()
            for term in term_words:
                if term[0] not in synonyms_dict['Word']:
                    synonyms_dict['Word'].append(term[0])
        term_index_dict['syn'].append(synonyms_dict)
        return term_index_dict

    def get_term_set(self, term_word):
        term_normalized = Term.normalization(term_word)
        term_hash_code = Term.get_hash_code(term_normalized)
        term_uid_list = Term.get_term_universal_ids(term_hash_code, )
        # Term_UID_List = [(68)]
        if term_uid_list:
            term_index = Term.get_term_index_list(term_uid_list, term_normalized, term_word)
            self._term_index_list.append(term_index)
        else:
            word_obj = {'token': term_word, 'type': 'Term', 'syn': []}
            self._term_index_list.append(word_obj)
        return self._term_index_list


def get_knowledge(term_word):
    term = Term()
    term_list = term.get_term_set(term_word)
    # print(json.dumps(termList, ensure_ascii=False, indent=2))
    # with open('TermIndex.txt', 'a', encoding='utf8') as jsonFile:
    #     json.dump(termList, jsonFile, ensure_ascii=False, indent=2)
    return term_list
