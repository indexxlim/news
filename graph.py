from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from itertools import compress
import copy
import numpy as np
from matplotlib.colors import ListedColormap
import matplotlib.pyplot as plt
import networkx as nx
from matplotlib import font_manager
import pandas as pd
from networkx.readwrite import json_graph
import community
import numpy as np
import scipy.sparse
import scipy.sparse.csgraph
import scipy.sparse as sp
from community import community_louvain as community


bannedwords = ['출처', '기자', '하지', '하기', '천억', '진짜', '이것', '저것', '누구', '무엇', '그것', '위해', '역시', '이런', '저런', '그런', '무슨',
               '누가', '요즘', '끼리', '가지', '정말', '보기', '하나', '이제', '어디', '라이', '아주', '바로', '자기', '그냥', '지금',
               '바로', '그냥', '다른', '이번', '해주', '언제', '때문', '완전', '이건', '보고', '얼마나', '모두', '너희', '우리', '당신', '해도',
               '해주',
               '하라', '건가', '요게', '그게', '이게', '그거', '저거', '지랄', '대가리', '이날', '오전', '관련', '동양', '입장', '오후', '당시',
               '가능',
               '과정', '경우', '생각', '진행', '이상', '시작', '가운데', '정도', '이후', '내용', '주장', '확인', '공개', '일부', '준비', '대상',
               '부분', '핵심',
               '상태', '입시', '보도', '결과', '동안', '시절', '차례', '다음', '포함', '학원', '사모', '정보', '언급', '지난', '지난달', '사이',
               '왼쪽', '처음', '상대',
               '결국', '직후', '개인', '얘기', '대신', '여기', '사실', '최근', '전날', '뉴스', '사람', '전문', '본인', '제기', '있다',
               '있다.', '무단', '배포', '니다', '월','년','.','_','-',',']

class Centrality:
    def __init__(self, input_g):
        """
        중심성을 산출하는 클래스입니다.
        :param input_g: nx graph
        """
        self.input_g = input_g

    def return_weighted_degree_centrality(self):
        w_d_centrality = {n: 0.0 for n in self.input_g.nodes()}
        for u, v, d in self.input_g.edges(data=True):
            w_d_centrality[u] += d['weight']
            w_d_centrality[v] += d['weight']
        else:
            return w_d_centrality

    def return_closeness_centrality(self):
        new_g_with_distance = self.input_g.copy()
        for u, v, d in new_g_with_distance.edges(data=True):
            d['weight'] = 1.0 / d['weight']
        return self.closeness_centrality_dev(new_g_with_distance)

    def return_betweenness_centrality(self):
        return nx.betweenness_centrality(self.input_g, weight='weight')

    def return_pagerank(self):
        return nx.pagerank(self.input_g, weight='weight')

    def return_eigenvector_centrality(self):
        return nx.eigenvector_centrality_numpy(self.input_g, weight='weight')

    def closeness_centrality_dev(self, G):  # nx.closeness_centrality 직접 수정
        A = nx.adjacency_matrix(G).tolil()
        D = scipy.sparse.csgraph.floyd_warshall( \
            A, directed=False, unweighted=False)
        n = D.shape[0]
        closeness_centrality = {}
        for r in range(0, n):

            cc = 0.0

            possible_paths = list(enumerate(D[r, :]))
            shortest_paths = dict(filter( \
                lambda x: not x[1] == np.inf, possible_paths))

            total = sum(shortest_paths.values())
            n_shortest_paths = len(shortest_paths) - 1.0
            if total > 0.0 and n > 1:
                s = n_shortest_paths / (n - 1)
                cc = (n_shortest_paths / total) * s
            closeness_centrality[r] = cc
        return closeness_centrality



def _calc_centrality(input_g, sort_by):
    """
    상위 노드를 정렬합니다.
    :param by: 정렬을 원하는 방식
        - frequency : 언급 빈도수로 정렬. co-occurrence matrix가 이미 빈도수로 정렬되어있어 별도의 처리x
        - pagerank : pagerank로 중심성이 높은 노드부터 내림차순으로 정렬
        - betwenness : betwennes로 중심성이 높은 노드부터 내림차순으로 정렬
        - weighted_degree : weighted degree로 중심성이 높은 노드부터 내림차순으로 정렬
        - closeness : closeness로 중심성이 높은 노드부터 내림차순으로 정렬
        - eigenvector : eigenvector로 중심성이 높은 노드부터 내림차순으로 정렬
    :return: centrality score of every node
    """

    cent = Centrality(input_g)
    if sort_by == 'frequency':
        score = None
    elif sort_by == 'pagerank':
        score = cent.return_pagerank()
    elif sort_by == 'betwenness':
        score = cent.return_betweenness_centrality()
    elif sort_by == 'weighted_degree':
        score = cent.return_weighted_degree_centrality()
    elif sort_by == 'closeness':
        score = cent.return_closeness_centrality()
    elif sort_by == 'eigenvector':
        score = cent.return_eigenvector_centrality()
    else:
        raise ValueError("잘못된 기준값입니다.")

    if score is not None:
        score = dict(sorted(score.items(), key=lambda x: x[1], reverse=True))

    return score

def drop_low_weighted_edge(inputG, above_weight=0.1):
    rG = nx.Graph()
    rG.add_nodes_from(inputG.nodes(data=True))
    edges = filter(lambda e: True if e[2]['nor_weight'] >= above_weight else False, inputG.edges(data=True))
    rG.add_edges_from(edges)

    # Delete isolated node를 모두 지운다.
    for n in inputG.nodes():
        if len(list(nx.all_neighbors(rG, n))) == 0:
            rG.remove_node(n)
        # print(n, list(nx.all_neighbors(rG, n)))
    return rG


def graph2json(articles, min_count=50, modularity=0.05, sort_by = 'closeness', top_n=50):
    # 작업 코드

    print('make graph')
    # CountVectorizer()를 이용한 term-document matrix 생성
    vectorizer = CountVectorizer(min_df=0.01,max_features = 1000) #stop_words=bannedwords,token_pattern=r'[^0-9]+')

    # CountVectorizer의 기본 설정: 두글자 이상 단어만 이용
    # 한글자 단어도 포함시킬 경우 윗줄 대신 아래 코드를 이용해 vecotrizer를 정의하세요
    # vectorizer = CountVectorizer(token_pattern=r"(?u)\b\w+\b")
    if len(articles) == 0:
        return ''
    elif len(articles) == 1:
        half_index = int(len(articles[0].split(' ')) / 2)
        articles = [' '.join(articles[0].split(' ')[:half_index]), ' '.join(articles[0].split(' ')[half_index:])]

    X = vectorizer.fit_transform(articles)
    print('X.shape : ', X.shape)

    # index를 바탕으로 단어를 정렬합니다. 추후 그래프의 node index와 단어를 매칭하는데 이용합니다.
    word2idx = dict(sorted(vectorizer.vocabulary_.items(), key=lambda x: x[1]))


    # co-occurrence matrix 생성 / 단어 빈도수 저장
    #if X.shape[0] > 5:
    #    X[X > 0] = 1  # 한 기사에 단어가 여러번 등장해도 한 번으로 설정(scaling)
    co_mat = X.T.dot(X)
    word_freq = co_mat.diagonal()
    min_max_freq = (word_freq - np.min(word_freq)) / (np.max(word_freq) - np.min(word_freq))  # min-max normalize
    word2freq = {w: min_max_freq[i] for w, i in word2idx.items()}
    co_mat.setdiag(0)



    num_art = [len(i) for i in articles]
    min_count = 0# sum(num_art)/10000

    # mean_num_art = sum(num_art) / len(num_art)
    #
    # if len(articles) < 100 or mean_num_art < 100:
    #     min_count = 0
    # elif len(articles) < 300 or mean_num_art < 300:
    #     min_count = 15
    # elif (mean_num_art) < 1000:
    #     min_count = 30
    # else:
    #     min_count = (mean_num_art) / 20



    #print('min_count: ', min_count)

    # 최소 등장 횟수 아래로 등장한 단어를 제외하고 co-occurrence matrix를 새롭게 생성
    # 상위 키워드로 정렬하는 경우 min_count를 50으로 설정
    if sort_by != 'frequency':
        min_count = min_count  # 최소 min_count (0으로 설정할 시 너무 오래 걸리는 issue 존재)
    co_mat = co_mat.multiply(co_mat > min_count)
    nonzero_idx = co_mat.getnnz(0) > 0  # 영벡터를 제외하고 공기행렬을 새롭게 정의
    co_mat = co_mat[nonzero_idx][:, nonzero_idx]
    label = zip(range(len(nonzero_idx)), list(compress(word2idx.keys(), nonzero_idx)))
    _idx2label = {i: w for i, w in label}
    # co-occurrence matrix의 index 별 word frequency

    word_frequency = np.array([word2freq[w] for w in _idx2label.values()])
    #### Buld
    G = nx.from_scipy_sparse_matrix(co_mat)
    score = _calc_centrality(G, sort_by)



    if sort_by != 'frequency':
        remove_idx = list(score.keys())[top_n:]
        G.remove_nodes_from(remove_idx)

        # 제거된 노드에 맞게 index 재 정렬
        _idx2label = {i: w for i, w in _idx2label.items() if i in list(G.nodes)}
        word_frequency = word_frequency[list(G.nodes)]

    # pos = nx.kamada_kawai_layout(G) #layout

    # edge 두께 설정
    weights = [G[u][v]['weight'] for u, v in G.edges()]
    # 엣지 두께의 차이가 너무 커 min-max scaling에서 max 대신 75 percentile 이용
    # print(weights)
    tmp_max = np.percentile(weights, 75)
    weights_normalized = (weights - np.min(weights)) / (tmp_max - np.min(weights) + 1e-9) + 0.2  # 0.2:보정값

    # 검색 기록에 따라 연관 단어 index 변화
    # ex) 첫 검색 : 남양유업 => 남양유업을 제외하고 1번 index부터 연관 검색어 지정
    # ex) 두번째 검색 : 소비자 => 남양유업, 소비자를 제외하고 2번 index부터 연관 검색어 지정
    # ex) 수정-> 무조건 키워드만 제거(한개씩)



    state = {}

    state['graph'] = G
    state['label'] = _idx2label
    state['word_frequency'] = word_frequency
    # state['position'] = pos
    state['weights_normalized'] = weights_normalized

    H = nx.relabel_nodes(state['graph'], state['label'])

    wf = 0
    for node in H.nodes():
        H.nodes[node]['weight'] = state['word_frequency'][wf]
        wf = wf + 1

    v = state['weights_normalized']

    nor_weights = (v - v.min()) / (v.max() - v.min())
    wf = 0
    for node in H.edges():
        H.edges[node]['nor_weight'] = nor_weights[wf]
        wf = wf + 1

    # modularity by threshold
    H2 = drop_low_weighted_edge(H, modularity)

    # Louvain algorithm
    group = community.best_partition(H2)
    wf = 0
    for node in H2.nodes():
        H2.nodes[node]['group'] = group[node]
        wf = wf + 1
    wf = 0
    for node in H2.edges():
        H2.edges[node]['group'] = group[node[0]]
        wf = wf + 1

    data = json_graph.node_link_data(H2)
    data_str = str(data).replace("'", '"')
    data_str = str(data_str).replace("False", 'false')
    data_str = str(data_str).replace("True", 'true')

    print('graphbuild_finish')


    return data_str, H


def tfidftable(bb):
    cv = CountVectorizer()  # max_features 수정
    tdm = cv.fit_transform(bb)

    ##TF-IDF
    tfidf = TfidfTransformer()
    tdmtfidf = tfidf.fit_transform(tdm)
    words = cv.get_feature_names()  # 단어 추출

    # sum tfidf frequency of each term through documents
    sums = tdmtfidf.sum(axis=0)

    # connecting term to its sums frequency
    data = []
    for col, term in enumerate(words):
        data.append((term, sums[0, col]))

    tfidftable = pd.DataFrame(data, columns=['키워드', 'TF-IDF'])
    tfidftable = tfidftable.set_index('키워드')
    tfidftable = tfidftable.sort_values('TF-IDF', ascending=False)
    return tfidftable.iloc[0:100].to_json(force_ascii=False)
