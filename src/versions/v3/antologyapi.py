"""Anthology api module."""
import time
from concurrent import futures

from flask import Blueprint
from flask import jsonify
from flask import request
from flask_cors import CORS
from flask_restplus import Api
from flask_restplus import Resource

from business.main import call_lemma
from business.main import get_from_ne
from business.main import get_from_terms
from similarity.sent_sim import *
from utilites.utility import chunk
from utilites.utility import find_tokens
from utilites.utility import normalization
from utilites.utility import not_in_chunk
from versions.v3.analysis.query import QueryAnalysis
from .analysis.relations import get_nearest_tax
from .analysis.relations import get_relations
from .analysis.relations import get_relations_with_id
from .analysis.relations import get_relations_with_name_id
from .analysis.relations import taxonomy_sorting
from .config import config

app = Blueprint(config['version'], config['version'])

CORS(app)

# Some info for swagger
flask_app = Api(app=app, version="1.0", title="Culture Ontology API", description="[Culture Ontology API Description]")
APP = flask_app.namespace('', description='Main APIs')


@APP.route('/taxonomy', methods=['GET'])
class Taxonomy(Resource):
    def get(self):
        start_time = time.time()

        alldata = []
        chunked = []
        query_chunk = []

        data_pram = {'query': '', 'call_num': 1, 'f_root': False, 'uid': 0}
        all_pram = request.args.to_dict()
        for i in all_pram.keys():
            if i.lower() in data_pram.keys():
                data_pram[i.lower()] = all_pram[i]

        query = data_pram['query']  # content['query']
        call_num = data_pram['call_num']
        flag_root = data_pram['f_root']  # content['flag_root']
        ids = data_pram['uid']

        lang = request.accept_languages.best
        try:
            call_num = int(call_num)
        except:
            pass
        if call_num == 1 or call_num == '1':
            analyze = call_lemma(query)
            if analyze:
                tokens = find_tokens(analyze)
            else:
                tokens = [normalization(t) for t in query.split()]
            for token in range(len(tokens)):
                if tokens[token] not in [v for v in chunked]:
                    the_chunk_list = chunk(token + 1, token, tokens[token], tokens, len(tokens), 1, None)
                    if the_chunk_list is not None:
                        if the_chunk_list[0] not in [v for v in chunked]:
                            query_chunk.append(the_chunk_list[:-1])
                            chunked += the_chunk_list[0].split()

            query_chunk = [x for x in query_chunk if x is not None and re.match('[ء-ي]', x[0])]
            with futures.ThreadPoolExecutor(3) as executor:
                task1 = executor.submit(get_from_terms, query_chunk)
                task2 = executor.submit(get_from_ne, query_chunk)
            print('T1: ', time.time() - start_time)

            alldata += task1.result()
            alldata += task2.result()
            first_entity_list = list(set([x[0]['token'] for x in alldata]))
            if first_entity_list:
                entity_list = not_in_chunk(first_entity_list)
            else:
                entity_list = []

            ########################################
            all_lemma = [(i[-1], i[0]) for i in analyze if '+' not in i[1]]
            all_lemma = all_lemma + [(i, i) for i in entity_list]
            print('T2: ', time.time() - start_time)
            queue_analysis = QueryAnalysis()
            qr = queue_analysis.query_relation(all_lemma, entity_list)
            qr = list(set(qr[:5] + qr[-5:]))
            print('T3: ', time.time() - start_time)
            entity_list = entity_list + qr
            taxonomy, list_of_topics3 = get_relations(entity_list, lang)
            print('T4: ', time.time() - start_time)
            taxonomy_output = taxonomy_sorting(entity_list, taxonomy)
            print('T5: ', time.time() - start_time)
            taxonomy_output.reverse()
            print('F_t: ', time.time() - start_time)

            return jsonify({'taxonomy': taxonomy_output, 'list_of_topics': list_of_topics3})

        elif call_num == 2 or call_num == '2':

            query_chunk = [[query, 1, 1]]
            with futures.ThreadPoolExecutor(12) as executor:
                task1 = executor.submit(get_from_terms, query_chunk)
                task2 = executor.submit(get_from_ne, query_chunk)

            alldata += task1.result()
            alldata += task2.result()
            first_entity_list = list(set([x[0]['token'] for x in alldata]))
            if first_entity_list:
                entity_list = not_in_chunk(first_entity_list)
            else:
                entity_list = []
            taxonomy, list_of_topics3 = get_relations(entity_list, lang)
            tax = []
            title_sim = []
            for i in taxonomy:
                for c in i:
                    tax.append(c)
            if tax:
                with futures.ThreadPoolExecutor(12) as executor:
                    task = executor.submit(get_nearest_tax, tax)

                title_sim = task.result()
            return jsonify({'taxonomy': title_sim, 'list_of_topics': list_of_topics3})

        elif (call_num == 3 or call_num == '3') and flag_root.lower() == 'true':
            ids = 92660
            taxonomy = get_relations_with_id(ids, lang)
            tax = []

            for i in taxonomy:
                if i not in tax:
                    tax.append(i)
            tax_ = [{'namedEntity1': t[1], 'relation': t[2], 'namedEntity2': t[3], 'subject': t[4], 'uid': t[0]} for t
                    in tax]
            return jsonify({'taxonomy': tax_})

        elif (call_num == 3 or call_num == '3') and flag_root.lower() == 'false':

            ids = int(ids)
            taxonomy, nn = get_relations_with_name_id(ids, lang)
            tax = []
            tax_ = []
            if taxonomy:
                if nn == 0:
                    for i in taxonomy:
                        if i not in tax:
                            tax.append(i)
                    tax_ = [
                        {'namedEntity1': t[1], 'relation': t[2], 'namedEntity2': t[3], 'subject': t[4], 'uid': t[0]}
                        for t in tax]
                else:
                    for i in taxonomy:
                        if i not in tax:
                            tax.append(i)
                    tax_ = [{'namedEntity1': t[0], 'relation': 'subclass of', 'namedEntity2': t[0], 'subject': None,
                             'uid': t[1]} for t in tax]

            return jsonify({'taxonomy': tax_[:20]})
