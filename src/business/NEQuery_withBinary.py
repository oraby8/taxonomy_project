"""Ne query module."""

import decimal
import re
from concurrent import futures

import pyodbc
import xxhash


class ConnectionString:
    """Connection string."""

    def __init__(self):
        self.con = pyodbc.connect(
            'Driver={ODBC Driver 17 for SQL Server};' 'Server=10.107.158.158;' 'Database=Ontology_KB;'
            'UID=SA;' 'PWD=EGY@2020;' "MultipleActiveResultSets=true")


class NE:
    """Ne class."""

    def __init__(self, ne_word: str):
        """
        Init method.

        Args:
            ne_word:  named entity word
        """
        self._NEDict = {'token': ne_word, 'type': 'NE', 'syn': []}
        self._synonymsDict = {'Word': []}
        self._NEIndexList = [self._NEDict]
        self._binaryList = []

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
    def get_ne_universal_ids(hashcode):
        connection = ConnectionString()
        cursor = connection.con.cursor()
        sql = "select Universal_ID from dbo.NE_KB where HashCode_Normalized = ?"
        val = hashcode
        cursor.execute(sql, val)
        tes = cursor.fetchall()
        return tes

    def get_ne_index_list(self, ne_uid_list, original_word):
        connection = ConnectionString()
        cursor = connection.con.cursor()
        if ne_uid_list:
            for neUId in ne_uid_list:
                sql = "select NE from dbo.NE_KB where Universal_ID = ? and NE != ?"
                val = (neUId[0], original_word)
                cursor.execute(sql, val)
                ne_words = cursor.fetchall()
                for ne in ne_words:
                    if ne[0] not in self._synonymsDict['Word']:
                        self._synonymsDict['Word'].append(ne[0])

                sql = "select distinct NE, TextualNE from dbo.NEIndex_1 where Universal_NEID = ? and NE != ?"
                val = (neUId[0], original_word)
                cursor.execute(sql, val)
                ne_index_words = cursor.fetchall()
                if ne_index_words:
                    for NEWord in ne_index_words:
                        if NEWord[0] not in self._synonymsDict['Word']:
                            self._synonymsDict['Word'].append(NEWord[0])
                        if NEWord[1] not in self._synonymsDict['Word'] and NEWord[1] is not None:
                            self._synonymsDict['Word'].append(NEWord[1])

                sql = "select distinct TextualNE from NEIndex_1 where Universal_NEID = ? and NE = ? and TextualNE != ?"
                val = (neUId[0], original_word, original_word)
                cursor.execute(sql, val)
                ne_index_t_words = cursor.fetchall()
                if ne_index_t_words:
                    for TNEWord in ne_index_t_words:
                        if TNEWord[0] not in self._synonymsDict['Word'] and TNEWord[0] is not None:
                            self._synonymsDict['Word'].append(TNEWord[0])

    def get_ne_binary(self, ne_uid_list, ne_normalized):
        connection = ConnectionString()
        cursor = connection.con.cursor()
        if ne_uid_list:
            for ne_uid in ne_uid_list:
                sql = "select Label2 from NEBinaryRelation where UID1 = ? and (RelationName = 'capital' or " \
                      "RelationName = 'head of state' or RelationName = 'notable work')" \
                      " and Deactivated is null"
                val = (ne_uid[0])
                cursor.execute(sql, val)
                result = cursor.fetchall()
                if result:
                    for res in result:
                        if res[0] not in self._binaryList and NE.normalization(res[0]) != ne_normalized:
                            # self._synonymsDict['Word'].append(res[0])
                            self._binaryList.append(res[0])

    def get_ne_set(self, ne_word):
        ne_normalized = NE.normalization(ne_word)
        ne_hash_code = NE.get_hash_code(ne_normalized)
        ne_uid_list = NE.get_ne_universal_ids(ne_hash_code)
        # NE_UID_List = [567, 51078]
        if ne_uid_list:
            # self.GetNEBinary(NE_UID_List, neNormalized)
            # self.GetNE_IndexList(NE_UID_List, neNormalized, neWord)
            param = [ne_uid_list, ne_normalized, ne_word]
            par1 = [ne_uid_list, ne_normalized]
            with futures.ThreadPoolExecutor(12) as executor:
                task2 = executor.submit(NE.get_ne_binary, *par1)
                task1 = executor.submit(NE.get_ne_index_list, *param)
            if self._binaryList:
                for binary in self._binaryList:
                    self._synonymsDict['Word'].append(binary)
            if self._synonymsDict['Word']:
                self._NEDict['syn'].append(self._synonymsDict)
        return self._NEIndexList


def get_knowledge(ne_word):
    ne = NE(ne_word)
    list_of_nes = ne.get_ne_set(ne_word)
    return list_of_nes
