import pyodbc


class DataBase:
    def __init__(self):
        self.connection = pyodbc.connect(
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=10.107.158.158;"
            "Database=Ontology_KB;"
            "UID=SA;"
            "PWD=EGY@2020"
        )

    def read_chunking(self, normalized_word):
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT Chunk_Normalized,Prefix,Is_NE,Is_Term,ChunkHashCode from dbo.Chunking WHERE Chunk_Normalized = "
            "N'{0}';".format(
                normalized_word))
        data = cursor.fetchall()
        return data

    def read_chunk_query(self, hashcode, term):
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT QueryChunk,Prefix,Is_NE,Is_Term from dbo.OntologyChunkQuery WHERE Deactivated=0 and "
            "ChunkHashCode={0} and QueryChunk=N'{1}';".format(
                hashcode, term))
        data = cursor.fetchall()
        return data

    def read_chunk_query_2(self, chunk_hash_code, term):
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT QueryChunk,Prefix,Is_NE,Is_Term,ChunkHashCode from dbo.OntologyChunkQuery WHERE Deactivated=0 and "
            "ChunkHashCode='{0}' and QueryChunk=N'{1}';".format(
                chunk_hash_code, term))
        data = cursor.fetchall()
        return data

    def get_word_id(self, chunk_hash_code):
        cursor = self.connection.cursor()
        cursor.execute("SELECT Universal_ID from dbo.NE_KB WHERE HashCode_Normalized='{0}' ;".format(chunk_hash_code))
        data = cursor.fetchall()
        return [x[0] for x in data]

    def relations(self, uid, lang):
        # []
        cursor = self.connection.cursor()
        if len(uid) > 1:
            query = ' or '.join([f'UID1={i}' for i in uid])
        else:
            query = f'UID1={uid[0]}'
        if lang == 'ar':
            cursor.execute(
                "SELECT Label1,RelationName_AR,Label2,Class from dbo.NEBinaryRelation WHERE ({0}) and RepeatedNE=0 "
                "and Deactivated is null;".format(
                    query))

        else:
            cursor.execute(
                "SELECT Label1,RelationName,Label2,Class from dbo.NEBinaryRelation WHERE ({0}) and RepeatedNE=0 and "
                "Deactivated is null;".format(
                    query))

        data = cursor.fetchall()
        if data:
            data = [list(i) for i in data]
            return data
        return data

    def relations_id(self, uid, lang):
        cursor = self.connection.cursor()
        if lang == 'ar':
            cursor.execute(
                "SELECT UID1,Label1,RelationName_AR,Label2,Class from dbo.NEBinaryRelation WHERE UID2={0} and "
                "RepeatedNE=0 and Deactivated is null and  RelationId != 4247;".format(
                    uid))

        else:
            cursor.execute(
                "SELECT UID1,Label1,RelationName,Label2,Class from dbo.NEBinaryRelation WHERE UID2={0} and "
                "RepeatedNE=0 and Deactivated is null and  RelationId != 4247;".format(
                    uid))
        data = cursor.fetchall()
        if data:
            data = [list(i) for i in data]
            return data
        return data

    def relations_id_2(self, uid, num, lang):
        if num == 2:
            cursor = self.connection.cursor()
            if lang == 'ar':
                cursor.execute(
                    "SELECT UID1,Label1,RelationName_AR,Label2,Class,IsTopic,taxonomy_tree_nodes from "
                    "dbo.NEBinaryRelation WHERE UID2={0} and RepeatedNE=0 and Deactivated is null and  RelationId != "
                    "4247;".format(
                        uid))

            else:
                cursor.execute(
                    "SELECT UID1,Label1,RelationName,Label2,Class,IsTopic,taxonomy_tree_nodes from "
                    "dbo.NEBinaryRelation WHERE UID2={0} and RepeatedNE=0 and Deactivated is null and  RelationId != "
                    "4247;".format(
                        uid))
            data = cursor.fetchall()
            if data:
                data = [list(i) for i in data]
                return data
            return data
        else:
            cursor = self.connection.cursor()
            if lang == 'ar':
                cursor.execute(
                    "SELECT UID2,Label1,RelationName_AR,Label2,Class,IsTopic,taxonomy_tree_nodes from "
                    "dbo.NEBinaryRelation WHERE UID1={0} and RepeatedNE=0 and Deactivated is null and  RelationId != "
                    "4247;".format(
                        uid))

            else:
                cursor.execute(
                    "SELECT UID2,Label1,RelationName,Label2,Class,IsTopic,taxonomy_tree_nodes from "
                    "dbo.NEBinaryRelation WHERE UID1={0} and RepeatedNE=0 and Deactivated is null and  RelationId != "
                    "4247;".format(
                        uid))
            data = cursor.fetchall()
            if data:
                data = [list(i) for i in data]
                return data
            return data

    def list_of_topics(self, uid):
        cursor = self.connection.cursor()
        cursor.execute("SELECT SubSubject,TF_IDF,IDF,TF from dbo.SubSubject_TFIDF2 WHERE UID={0};".format(uid))
        data = cursor.fetchall()
        if data:
            data = [list(i) for i in data]
            return data
        return data

    def sub_subject_tfidf(self, ne):
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT NE_Label,UID from dbo.SubSubjectTFIDF_NEView WHERE SubSubject=N'{0}' order by TF_IDF DESC;".format(
                ne))
        data = cursor.fetchall()
        if data:
            data = [list(i) for i in data[:5]]
            return data
        return data

    def sub_subject_ne_occurrence(self, ne):
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT NE,UID from dbo.SubSubject_NEOccurrence_noQuantity_Other_Date WHERE SubSubject=N'{0}' order by "
            "Occurrence DESC;".format(
                ne))
        data = cursor.fetchall()
        if data:
            data = [list(i) for i in data[:5]]
            return data
        return data

    def list_of_topics_2(self, uid):
        cursor = self.connection.cursor()
        if len(uid) > 1:
            query = ' or '.join([f'UID={i}' for i in uid])
        else:
            query = f'UID={uid[0]}'
        cursor.execute(
            "SELECT SubSubject,Occurrence,"
            "IsCommon from dbo.SubSubject_NEOccurrence_noQuantity_Other_Date WHERE ({0});".format(
                query))
        data = cursor.fetchall()
        if data:
            data = [list(i) for i in data]
            return data
        return data

    def relations_2(self, uid, lang):
        cursor = self.connection.cursor()
        if lang == 'ar':
            cursor.execute(
                "SELECT Label1,RelationName_AR,Label2,Class from dbo.NEBinaryRelation WHERE UID2={0} and RepeatedNE=0 "
                "and Deactivated is null;".format(
                    uid))

        else:
            cursor.execute(
                "SELECT Label1,RelationName,Label2,Class from dbo.NEBinaryRelation WHERE UID2={0} and RepeatedNE=0 "
                "and Deactivated is null;".format(
                    uid))
        data = cursor.fetchall()
        if data:
            data = [list(i) for i in data]
            for i, x in enumerate(data):
                data[i].append(None)
            return data
        return data

    def query_relation(self, lemma):
        cursor = self.connection.cursor()

        cursor.execute(
            "SELECT Relation_name from dbo.Relation_Aliases WHERE Relation_Name_Normalized=N'{0}';".format(lemma))
        data = cursor.fetchall()
        if not data:
            cursor.execute(
                "SELECT Relation_name from dbo.Relation_Aliases WHERE Relation_Alias_Normalized=N'{0}';".format(lemma))
            data = cursor.fetchall()
        if data:
            data = [list(i) for i in data][0]
        return data

    def query_relation_lemma(self, lemma):
        cursor = self.connection.cursor()
        cursor.execute("SELECT Relation_name from dbo.Relation_Aliases WHERE Relation_Name_Lemma=N'{0}';".format(lemma))
        data = cursor.fetchall()
        if not data:
            cursor.execute(
                "SELECT Relation_name from dbo.Relation_Aliases WHERE Relation_Alias_Lemma=N'{0}';".format(lemma))
            data = cursor.fetchall()

        if data:
            data = [list(i) for i in data][0]
        return data

    def query_find_relation(self, uid, ne_name):
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT Label2 from dbo.NEBinaryRelation WHERE UID1={0} and RelationName='{1}' and RepeatedNE=0 and "
            "Deactivated is null;".format(
                uid, ne_name))
        data = cursor.fetchall()
        if data:
            data = [list(i) for i in data]
            return data
        return data
