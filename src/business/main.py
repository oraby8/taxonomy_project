import requests

from .NEQuery_withBinary import NE
from .TermQuery_NoDocs import Term


def call_lemma(token):
    url = 'http://10.107.158.147:8000/getPOS?inputText={0}'.format(token)
    response = requests.get(url)
    content = response.json()
    try:
        query_words = [[i['lemma'], i['token'], i['tokenpos'], i['word']] for i in content['QueryToken']]
    except:
        query_words = []
    return query_words


def get_from_terms(query_chunk):
    term_list = []
    terms = [i[0] for i in query_chunk if i is not None and i[2] == 1]
    if terms:
        for i in terms:
            term = Term()
            term_list.append(term.get_term_set(i))
    return term_list


def get_from_ne(query_chunk):
    ne_list = []
    nes_list = [i[0] for i in query_chunk if i is not None and i[1] == 1]
    if nes_list:
        for i in nes_list:
            ne = NE(i)
            ne_list.append(ne.get_ne_set(i))
    return ne_list
