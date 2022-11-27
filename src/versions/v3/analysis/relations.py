"""Relation module."""
import time
from concurrent import futures
from itertools import permutations

from database.databaseconnection import DataBase
from similarity.sent_sim import get_nearest
from similarity.tfidf import get_tfidf_sims
from utilites.utility import hash_code
from utilites.utility import normalization


def ranking_list_of_topic(topics_list):
    matching_topic = {}
    for i in topics_list:
        if i[0] not in matching_topic:
            matching_topic[i[0]] = 0
        else:
            matching_topic[i[0]] = matching_topic[i[0]] + 1
    list_of_topics = []
    for topic in topics_list:
        if list_of_topics:
            if topic[0] not in [i[0] for i in list_of_topics]:
                list_of_topics.append(topic + [matching_topic[topic[0]]])
        else:
            list_of_topics.append(topic + [matching_topic[topic[0]]])

    list_of_topics = sorted(list_of_topics, key=lambda x: x[-1], reverse=True)
    list_of_topics = [i[0] for i in list_of_topics]
    return list_of_topics[:5]


def get_uid(word, database):
    hashcode = hash_code(normalization(word))
    uid = database.get_word_id(hashcode)
    return uid, word


def relation_from_db(ids, lang, database, word):
    relations = []
    all_word = []
    li3 = None
    start_time = time.time()
    out = database.relations(ids, lang)
    if out:
        li3 = database.list_of_topics_2(ids)
        print('DB_time: ', time.time() - start_time)
        li3 = [top + [word] for top in li3]
        if li3:
            li3 = sorted(li3, key=lambda x: x[1], reverse=True)[:5]
        relations = relations + out
        all_word.append(1)
    return li3, relations, all_word


def get_relations(words, lang):
    start_time = time.time()
    relations = []
    list_of_topics_3 = []
    database = DataBase()
    all_word = []
    all_uid = [get_uid(word, database) for word in words]
    for i in all_uid:
        with futures.ThreadPoolExecutor(5) as executor:
            relation_task = executor.submit(relation_from_db, i[0], lang, database, i[1])
        li3, rela, iw = relation_task.result()
        list_of_topics_3 = list_of_topics_3 + li3 if li3 else list_of_topics_3
        relations = relations + rela if rela else relations
        all_word = all_word + iw if iw else all_word
    print('T4.2: ', time.time() - start_time)
    if not all_word:
        for i in all_uid:
            out = database.relations_2(i, lang)
            if out:
                relations.append(out)
    print('T4.5: ', time.time() - start_time)
    return relations, ranking_list_of_topic(list_of_topics_3)


def get_relations_with_id(ids, lang):
    database = DataBase()
    out = database.relations_id(ids, lang)

    return out


def get_relations_with_name_id(ids, lang):
    database = DataBase()
    out = []
    data = database.relations_id_2(ids, 2, lang)
    is_topic = 'Yes_LastTopic'
    if not data:
        data = database.relations_id_2(ids, 1, lang)
        if data:
            words = [i[-1] for i in data if i[-2] == is_topic]
            if words:
                data = database.sub_subject_tfidf(words[0])
                data = data + database.sub_subject_ne_occurrence(words[0])
                # read data from prancer table
                out = [i for i in data]
                return out, 1
    else:
        out = [i for i in data]
    return out, 0


def taxonomy_sorting(entity_list, taxonomy):
    final_set = []
    for i in taxonomy:
        if i not in final_set:
            final_set.append(i)
    ent = [list(f) for f in list(permutations(entity_list, 2))]
    taxonomy_out = sorted(final_set, key=lambda x: [normalization(x[0]), normalization(x[2])] in ent, reverse=True)
    taxonomy_out = [{'namedEntity1': t[0], 'relation': t[1],
                     'namedEntity2': t[2], 'subject': t[3]} for t in taxonomy_out]
    return taxonomy_out


def get_nearest_tax(taxonomy_list):
    database = DataBase()
    all_uid = ''
    out = []
    hashcode = hash_code(normalization(taxonomy_list[0][2]))
    uid = database.get_word_id(hashcode)
    if uid:
        all_uid = ' '.join([str(i) for i in uid])

    for i in taxonomy_list:
        try:
            sim = get_nearest(i[2], 1)[:3]
        except:
            sim = []
        try:
            sim2 = get_tfidf_sims(all_uid, 3)[:3]
        except:
            sim2 = []
        all_similarity = sim + sim2
        out.append(
            [{'namedEntity1': i[0], 'relation': i[1], 'namedEntity2': i[2], 'subject': i[3],
              'titlesim': all_similarity}])
    return out
