"""Utility module."""
import decimal
import re

import xxhash

from database.databaseconnection import DataBase


def chunk(ii, i, token, tokens, length, id_, hash_code_token=None, dx=[], flag=None):
    dm = dx
    hash_code_token = hash_code_token
    database = DataBase()
    if hash_code_token is None:
        id_ = 1
    if id_ == 1:
        data_chunking = database.read_chunking(token)

    else:
        data_chunking = database.read_chunk_query_2(hash_code_token, token)

    if data_chunking:
        dm.append(list(data_chunking[0][0:2]))
        chunked_token, prefix, is_ne, is_term, chunk_hash_code = data_chunking[0][0], \
                                                                 data_chunking[0][1], \
                                                                 data_chunking[0][2], \
                                                                 data_chunking[0][3], \
                                                                 data_chunking[0][4]

        if int(prefix) == 0:
            output = database.read_chunk_query(chunk_hash_code, token)
            if not output:
                output = database.read_chunking(token)
                if output:
                    return [output[0][0], output[0][2], output[0][3], ii]
            else:
                return [output[0][0], output[0][2], output[0][3], ii]
        if int(prefix) == 1:
            if length > ii:  # length> ii+1 (14/10)
                temp = ii + 1
                if flag == 1:
                    sent_text = ' '.join(token.split() + [tokens[temp - 1]])  # -1 (14/10)
                    sent_text = sent_text.replace('+ ', '')
                    sent_text = sent_text.replace(' +', '')
                else:
                    sent_text = ' '.join(tokens[i:temp])
                return chunk(temp, i, sent_text, tokens, length, 3, chunk_hash_code, dm)
            else:

                if len(token.split()) > 1:
                    if dm:
                        dm.reverse()

                        s = [e for e in dm if e[1] == 2]
                        if s:
                            d = s[0][0]
                        else:
                            d = token
                    else:
                        d = token
                    output = database.read_chunk_query_2(chunk_hash_code, d)
                    if output:
                        return [output[0][0], output[0][2], output[0][3], ii]

        if int(prefix) == 2:
            if length > ii:
                temp = ii + 1
                if flag == 1:
                    sent_text = ' '.join(token.split() + [tokens[temp - 1]])
                    sent_text = sent_text.replace('+ ', '')
                    sent_text = sent_text.replace(' +', '')
                else:
                    sent_text = ' '.join(tokens[i:temp])
                return chunk(temp, i, sent_text, tokens, length, 2, chunk_hash_code, dm)
            else:
                if len(token.split()) > 1:
                    output = database.read_chunk_query_2(chunk_hash_code, token)

                    if output:
                        return [output[0][0], output[0][2], output[0][3], ii]
                else:
                    output = database.read_chunking(token)
                    if output:
                        return [output[0][0], output[0][2], output[0][3], ii]
    else:
        if '+' in token:
            # has been updated in 8/03/2021
            if length > ii:
                t = ' '.join(tokens[i:ii + 1])
                t = t.replace('+ ', '')
                t = t.replace(' +', '')
                return chunk(ii + 1, i, t, tokens, length, 4, hash_code_token, dm, 1)

        if id_ == 4:
            t = ' '.join(tokens[i:ii])
            t = t.replace('+ ', ' ')
            t = t.replace(' +', ' ')

            return chunk(ii, i, t, tokens, length, 2, hash_code_token, dm, 1)

        if id_ == 2:
            if len(token.split()) > 1:
                sent_text = token.split()
                output = database.read_chunk_query_2(hash_code_token, ' '.join(sent_text[:-1]))
                if output:
                    return [output[0][0], output[0][2], output[0][3], ii]

            output = database.read_chunking(' '.join(tokens[i:ii - 1]))
            if output:
                return [output[0][0], output[0][2], output[0][3], ii]
        # has been updated in 14/12/2020
        else:
            output = database.read_chunking(' '.join(tokens[i:ii - 2]))
            if output:
                return [output[0][0], output[0][2], output[0][3], ii]


def hash_code(word):
    word = decimal.Decimal(xxhash.xxh64(word).intdigest())
    return word


def normalization(text):
    text = re.sub("[إأٱآا]", "ا", text)
    text = re.sub("ى", "ي", text)
    text = re.sub("ة", "ه", text)
    return text


def not_in_chunk(list_of_tokens):
    final_tokens = []
    list_of_tokens = sorted(list_of_tokens, key=len, reverse=False)
    for i, j in enumerate(list_of_tokens):
        flag = False
        for m in list_of_tokens[i + 1:]:
            if j in m:
                flag = True
                break
        if not flag:
            final_tokens.append(j)
    return final_tokens


def has_duplicates(my_list):
    m = []
    for index in range(len(my_list)):
        f = False
        if index < len(my_list) - 1:
            item = my_list[index]
            if item == my_list[index + 1]:
                f = True
        if not f:
            m.append(my_list[index])
    return m


def find_tokens(analyze):
    tokens = [normalization(i[1]) for i in analyze]
    token_pos = [i[2] for i in analyze]
    word = [i[3] for i in analyze]

    q = []
    queue = []
    for i in range(len(tokens)):
        t = tokens[i]
        f = True
        if (token_pos[i] not in ['conj+', 'prep+']) or ('ل+' in tokens[i]):
            t = t.replace('+', ' ')
            t = t.replace('+', ' ')
            f = False
        if token_pos[i] == 'det+' and t[:-1] not in word[i]:
            t = 'ل '
            f = False

        queue.append([t, normalization(word[i]), f])
        q.append(t)

    word = [normalization(i[1]) for i in queue]
    word = has_duplicates(word)
    r = []
    for m in word:
        s = [i[2] for i in queue if normalization(i[1]) == m]
        if len(s) > 1:
            if len(list(set(s))) > 1 and True in s:

                r = r + [i[0] for i in queue if normalization(i[1]) == m]
            else:
                r = r + [m]
        else:
            r = r + [m]
    sent = ' '.join(r)
    sent = sent.replace('  ', '')
    tokens = sent.split()
    return tokens
