import pandas as pd
from gensim.corpora import Dictionary
from gensim.models import TfidfModel
from gensim.similarities import SparseMatrixSimilarity

from resources.resources_parms import Similarity_params

nes_docs = pd.read_excel(Similarity_params.main_path + '/tfidf/nes_docs.xlsx')
title_keys = pd.read_excel(Similarity_params.main_path + '/tfidf/titles_keys.xlsx')

dct = Dictionary.load(Similarity_params.main_path + '/tfidf/tfidf_dict.dict')
tfidf = TfidfModel.load(Similarity_params.main_path + '/tfidf/tfidf.model')
index = SparseMatrixSimilarity.load(Similarity_params.main_path + '/tfidf/tfidf_sparse_index.index')


def get_tfidf_sims(q, top_n):
    q_vector = dct.doc2bow(q.split())
    sim = index[tfidf[q_vector]]
    idx = sim.argsort()[-top_n:][::-1]
    res_df = pd.DataFrame(nes_docs['DocKey'].iloc[idx])

    res_df['Title'] = list(title_keys['Title'].loc[title_keys['DocKey'].isin(list(res_df['DocKey']))])

    res_df["sim"] = sim[idx]
    return res_df.to_dict('records')
