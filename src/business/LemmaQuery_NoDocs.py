"""Lemma query module."""

import decimal
import json
import time

import pyodbc
import xxhash


class ConnectionString:
    """Connection."""

    def __init__(self):
        self.con = pyodbc.connect(
            'Driver={ODBC Driver 17 for SQL Server};' 'Server=10.107.158.158;' 'Database=Ontology_KB;'
            'UID=SA;' 'PWD=EGY@2020;' "MultipleActiveResultSets=true")


class Lemma:
    """Lemma class."""

    def __init__(self):
        self._LemmaIndexList = []

    def GetHashCode(self, lemma):
        wordHashCode = decimal.Decimal(xxhash.xxh64(lemma).intdigest())
        return wordHashCode

    def GetLemma_IDs(self, lemma_HashCode):
        connection = ConnectionString()
        cursor = connection.con.cursor()
        lsidLemmaList = []

        # first case
        sql = "select LSID from dbo.Ontology_Concept where HashCode_Diac = ? and Ref_Source = 'Original'"
        val = lemma_HashCode
        cursor.execute(sql, val)
        lsid_list = cursor.fetchall()
        if lsid_list:
            for lsid in lsid_list:
                sql = "select LemmaID, Synonyms from dbo.Ontology_Concept where LSID = ? and is_common = 1"
                val = lsid[0]
                cursor.execute(sql, val)
                lsid_lemma = cursor.fetchall()
                # if lsid_lemma:
                if len(lsid_lemma) > 2:
                    for lsidlemma in lsid_lemma:
                        lsidLemmaObj = [lsidlemma[0], lsidlemma[1]]
                        lsidLemmaList.append(lsidLemmaObj)
                else:
                    sql = "select LemmaID, Synonyms from dbo.Ontology_Concept where LSID = ?"
                    val = lsid[0]
                    cursor.execute(sql, val)
                    all_lemma = cursor.fetchall()
                    if all_lemma:
                        for lemma in all_lemma:
                            lsidLemmaObj = [lemma[0], lemma[1]]
                            lsidLemmaList.append(lsidLemmaObj)
        else:
            # case 2
            sql = "select LSID from dbo.Ontology_Concept where HashCode_Diac = ?"
            val = lemma_HashCode
            cursor.execute(sql, val)
            result = cursor.fetchall()

            if result:
                for ls in result:
                    # lsidList.append((ls[0], -1))
                    sql = "select LemmaID, Synonyms from dbo.Ontology_Concept where LSID = ?"
                    val = (ls[0])
                    cursor.execute(sql, val)
                    lsid_lemma = cursor.fetchall()
                    if lsid_lemma:

                        for lsidlemma in lsid_lemma:
                            lsidLemmaObj = [lsidlemma[0], lsidlemma[1]]
                            lsidLemmaList.append(lsidLemmaObj)
                # if lsidLemmaList:
                #     for res in result:
                #         lsidLemmaList.append(res)
                #         break
                # else:
                #     # case 3 return all lsid without lemmaid
                #     return lsidList
        return lsidLemmaList

    def GetLemma_IndexList(self, LemmaIdList, lemmaDiac, word):
        connection = ConnectionString()
        cursor = connection.con.cursor()
        LemmaIndexDict = {'token': word, 'type': 'Lemma', 'Lemma': lemmaDiac, 'syn': []}
        # synonymsDict = {'Word': [], 'doc_num': []}
        synonymsDict = {'Word': []}

        # if self._lsidLemmaList:
        for lemmaList in LemmaIdList:
            if lemmaList[0] == 0:
                synonymsDict['Word'].append(lemmaList[1])
                continue

            sql = "select distinct Word from dbo.LemmaIndex where LemmaID = ? and Word !=?"
            val = (lemmaList[0], word)
            cursor.execute(sql, val)
            lemmaIndex_Words = cursor.fetchall()

            if lemmaIndex_Words:
                for lemmaWord in lemmaIndex_Words:
                    if lemmaWord[0] not in synonymsDict['Word']:
                        synonymsDict['Word'].append(lemmaWord[0])

            # sql = "select distinct DocId from dbo.LemmaIndex where LemmaID = ?"
            # val = (lemmaList[1])
            # cursor.execute(sql, val)
            # LemmaIndex_Docs = cursor.fetchall()
            #
            # for lemmaDoc in LemmaIndex_Docs:
            #     if lemmaDoc[0] not in synonymsDict['doc_num']:
            #         synonymsDict['doc_num'].append(lemmaDoc[0])

        LemmaIndexDict['syn'].append(synonymsDict)
        return LemmaIndexDict

    def Words_byLSID(self, lsidList):
        connection = ConnectionString()
        cursor = connection.con.cursor()
        sql = "select distinct Word from dbo.LemmaIndex where LSID = ? and Word != ?"
        val = (lsidList[0], lsidList[1])
        cursor.execute(sql, val)

    def Words_byLemmaID(self, lemmaList):
        connection = ConnectionString()
        cursor = connection.con.cursor()
        # sql = "select LemmaID, Word, DocId from dbo.LemmaIndex where LSID = ? order by Word"
        sql = "select distinct Word from dbo.LemmaIndex where LemmaID = ? and Word !=?"
        val = (lemmaList[0], lemmaList[1])
        cursor.execute(sql, val)

    def GetLemma_Set(self, lemmaDiac, word):
        lemmaHashCode = self.GetHashCode(lemmaDiac)
        LemmaIDs_List = self.GetLemma_IDs(lemmaHashCode)
        # LemmaIDs_List = [358, (251,), (85,), (968)] h[0]
        if LemmaIDs_List:
            lemmIndex = self.GetLemma_IndexList(LemmaIDs_List, lemmaDiac, word)
            self._LemmaIndexList.append(lemmIndex)
        else:
            # word_obj = {'token': word, 'type': 'Lemma', 'Lemma': lemmaDiac, 'syn': [], 'listofdoc': []}
            word_obj = {'token': word, 'type': 'Lemma', 'Lemma': lemmaDiac, 'syn': []}
            self._LemmaIndexList.append(word_obj)
        return self._LemmaIndexList


def Get_Knowledge(lemmaDiac, word):
    lem = Lemma()
    lemmaList = lem.GetLemma_Set(lemmaDiac, word)
    print(json.dumps(lemmaList, ensure_ascii=False, indent=2))
    # with open('LemmaIndex.txt', 'a', encoding='utf8') as jsonFile:
    #     json.dump(lemmaList, jsonFile, ensure_ascii=False, indent=2)
    return lemmaList


if __name__ == '__main__':
    start = (time.time())
    word = 'تأكيد'
    lemmaDiac = 'تَأْكِيد'
    # for lem in lemmaDiac:
    lemmaResult = Get_Knowledge(lemmaDiac, word)
    print(lemmaResult)
    print(time.time() - start)
