"""sentences similarity module."""

import json
import re

import pandas as pd
import tensorflow_hub as hub
from annoy import AnnoyIndex

from resources.resources_parms import Similarity_params

# loading dataset
df1 = pd.read_excel(Similarity_params.main_path + '/bat2_125k.xlsx')

# loading base model
embed_text = hub.load(Similarity_params.main_path['main_path'] + '/universal-sentence-encoder-multilingual_3')
# embed_text = hub.load('https://tfhub.dev/google/universal-sentence-encoder-multilingual/3')


index1 = AnnoyIndex(512, 'dot')
index = AnnoyIndex(512, 'dot')

# loading dicts
with open(Similarity_params.main_path + "/titles_dic_125k.txt", "r") as f:
    dic_titles = json.load(f)

with open(Similarity_params.main_path + "/titles_meta_dic_125k.txt", "r") as f:
    dic_meta = json.load(f)

# conversing keys to ints
dic_titles = {int(k): v for k, v in dic_titles.items()}
dic_meta = {int(k): v for k, v in dic_meta.items()}

# loading indices
index1.load(Similarity_params.main_path + '/index_125k_60t_titles+meta.ann')
index.load(Similarity_params.main_path + '/index_125k_60t_titles.ann')


def get_nearest(query, job_id, n=10, k=10000, dist=True, threshold=.6):
    q = re.sub('[^ء-يA-Za-z0-9]', ' ', str(query))
    v = embed_text(q)[0]
    if job_id == 1:
        results_ids = index.get_nns_by_vector(v, n=24, search_k=k, include_distances=dist)
        titles = [dic_titles[idx] for idx in results_ids[0]]
        sims = results_ids[1]
        doc_keys = list(df1.iloc[list(results_ids[0])]['DocKey'])
        sim_dic = {'title': titles, 'ref_doc_num': doc_keys, 'percentage': sims, }
        dic = (pd.DataFrame(sim_dic)).to_dict('records')
        return dic

    if job_id == 2:
        results_ids = index.get_nns_by_vector(v, n=n, search_k=k, include_distances=dist)
        titles = [dic_titles[idx] for idx in results_ids[0]]
        sims = results_ids[1]
        doc_keys = list(df1.iloc[list(results_ids[0])]['DocKey'])
        sim_dic = {'title': titles, 'ref_doc_num': doc_keys, 'percentage': sims, }
        sim_df = pd.DataFrame(sim_dic)
        sim_df = sim_df[sim_df['percentage'] >= .8]
        sim_dic = sim_df.to_dict('records')
        return sim_dic

    if job_id == 3:
        results_ids = index1.get_nns_by_vector(v, n=n, search_k=k, include_distances=dist)
        titles = [dic_meta[idx] for idx in results_ids[0]]
        sims = results_ids[1]
        doc_keys = list(df1.iloc[list(results_ids[0])]['DocKey'])
        sim_dic = {'title': titles, 'ref_doc_num': doc_keys, 'percentage': sims, }
        sim_df = pd.DataFrame(sim_dic)
        sim_df = sim_df[sim_df['percentage'] >= threshold]
        sim_dic = sim_df.to_dict('records')
        return sim_dic
