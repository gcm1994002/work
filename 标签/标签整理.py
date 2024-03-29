import collections
from typing import Union
import pandas as pd
import re
import random
import func  # 自定义的函数文档
import jieba
import time


def dict_trans_list(enter_argument: list, input_list: dict) -> dict:  # 字典转列表，通过书名分词把包含相同分词的isbn聚合。
    """
    通过比对两个值的分词其中一部分来判断两者是否为异名同义。
    传入列表和储存输出值的外部字典，传入的参数格式是[{isbn:[book_name]}]，遍历传入的列表，读取其中的字典，把字典存入输出列表中，
    每次遍历输出的值，比对该字典的键是否已经存在于输出列表的键中，如果存在，把遍历到的键值对和已存在字典的键值对进行去重，合并。
    最后把输出键值对的值转成str格式，转成Dataframe的时候不会因为列表长度太长，而导致表格数据过多。
    """
    for argument_dict in enter_argument:
        # 输入的字典是{isbn:书名分词} 参照 dict_index函数来操作,遍历到的值存入列表中,有相同的,加进去.
        for k, v in argument_dict.items():
            if k not in input_list.keys():
                input_list[k] = v
            else:
                for sample_element in v:
                    if sample_element not in input_list[k]:
                        input_list[k].append(sample_element)
                    else:
                        pass
    for k, v in input_list.items():
        input_list[k] = str(v)

    return input_list


def dict_cnt(dict0_total_list: list, dict_index_: dict) -> dict:  # 索引匹配 传入一个列表。遍历列表内的值。即可实现传入多个列表的目的。
    """输入一个值的格式是 list({isbn:str(字典)},{isbn:str(字典)},{isbn:str(字典)})的列表
    对最内侧的列表进行遍历.用一个新的空字典来储存每次遍历到的值和索引.形成一个{值:[isbn]}的列表.如果值在里边这个字典中,那么往值的列表增加isbn,
    如果不在列表中,那么把它放进列表中.

    dict_total 储存输出结果的字典
    dict_list_total 传入带有参数的列表
    dict0_total_list 单个参数的数字典集合
    index_ isbn号
    word 每行字典的键值对
    """
    cnt = 0
    dict_total = dict()

    # '这里相当于是一次遍历,把出现这个书名的isbn都放在一起.'
    for dict_list_total in dict0_total_list:  # 从输入值拆分参数,只要有一个列表就行.不用写死传入多少个参数
        for index_, word in dict_list_total.items():  # 对参数进行遍历,输出是{ISBN(int格式) : 字典(str)}
            cnt += 1
            if type(word) == str:  # 如果是从前边继承来的值,那么这里应该直接就是dict.不用转格式,所以要写if...else
                dict_v = eval(word)
            else:
                dict_v = word
            # '参数k是格式值 参数v是格式后的值'
            for k, v in dict_v.items():  # 对每行参数字典化后键值在输出的字典中做匹配.
                # '如果要写正则,可以加在这里.但似乎加了模糊匹配,每个值都要遍历一次列表.那就是n^2，所以暂时放弃'
                # '如果值在输出的字典中.说明已经有这个元素,所以往对应的键值中加上索引. 因为列表有两个元素,所以要遍历两次.'
                if v in dict_total.keys():
                    dict_total[v].append(dict_index_[index_])
                # '如果值不在输出字典中，那么把键赋值列表。'
                else:
                    dict_total[v] = [dict_index_[index_]]

    # '格式转换,列表数据转成字符串,在输出表格时结果就只有两列.而不是数十列.'
    for k, v in dict_total.items():
        dict_total[k] = str(list(set(v)))
    return dict_total


def dict_index(list1: Union[list, str], data_dict: pd.DataFrame) -> dict:  # 输入的是要找索引的文本列表。以及列表对应索引的值。
    """输入字符，转为列表。以列表的值通过索引获取对应两个信息"""

    index_list = eval(list1)

    res = []
    dict1 = {}
    for index_ in index_list:
        res.extend(list((data_dict.loc[data_dict['isbn'] == index_])['书名键'].values))
        res.extend(list((data_dict.loc[data_dict['isbn'] == index_])['书名值'].values))
    for index_ in index_list:
        dict1[index_] = list(set(res))
    # print(list1, dict1)
    return dict1


def switch(j: Union[str, list], f: Union[str, list], lis1: list) -> Union[str, list]:
    """
    本来是为了应对简繁的.但后来发现可以用于为两个列表进行分列匹配.输入的都是字符串
    把参数a转成列表,把参数b也转成列表.
    20230110 因为之前没有往函数中定义储存的地方。所以当出现带分号的数据时，
    每次调用返回的值都没办法传递给下一个把输入的两个参数转变成列表,注意转换后的格式,可能出现通过一条逻辑转成列表.
    所以要针对能想到的可能导致转换失败的情况编写判断逻辑和转换逻辑
    """

    try:
        # '如果分隔符存在字符串中.以分隔符拆分成列表,否则直接把参数赋值到列表中'
        for n in range(len(j)):
            # '从n的位置开始向参数b分割成的列表遍历'
            for b_n in range(n, len(f)):
                # '因为是把这一行的值都进行关联,所以是不等于,且不为空.'
                if j[n] != f[b_n] and f[b_n] != '' and j[n] not in lis1:
                    dic = dict()
                    dic[j[n]] = f[b_n]
                    if dic not in lis1:
                        lis1.append(dic)

    except:
        return []
    '%s 是排除了可能出现的纯数字结果导致报错.'
    return ";".join(['%s' % id1 for id1 in lis1])


def str_replace(s: str, r: str) -> str:  # 替换
    if pd.isna(s):
        return ''
    else:
        return re.sub(r, '', s)


def random_int(n: int) -> int:
    """
    随机抽取特定比例的数据来校验效果，默认为100,
    """
    return random.randint(1, n)


def set_dict(name: str):
    """输出的是输入参数的大写版本.用来将数据格式统一"""
    dict1 = {}
    g_name = func.daxiaoxie(name)
    dict1[name] = g_name
    return dict1


def ownership_duplicate(a: [str, list], b: list) -> list:  # 判断归属
    """
    直接把字符化的字典赋值到列表时,系统会自动补充斜杠
    输入的参数包括无分号的字符串,单独的字典字符串,一个空列表,带分号的字符串
    注意越限，和特例，避免报错，或者对报错应该要有应对措施，不要让它直接跳出运转。
    """
    try:
        if a == [''] or pd.isna(a) or a == '':
            return b

        # '把输入的参数转成列表，在函数外的前一步已经将输入的内容转成字符串了。'
        # '这里相当于是把字符串重新转为字典'
        team_lis = a.split(';') if ';' in a else [eval(a)]

        # '从lis中输出值，逐个比对是否存在传递结果的外部列表中。'
        for i1 in team_lis:
            i_dict = eval(i1)
            if i_dict not in b and pd.notna(i_dict):
                b.append(i_dict)  # 两个列表合并
        return b
    except:
        pass


def book_merge(enter1, enter2):
    """把帖子关联书籍和书籍信息进行合并，取数时并未直接取书籍ID，所以只能自行处理。"""
    df = pd.read_excel(enter1).astype('str')  # 书籍信息库，转成字符格式，避免ISBN编码被识别成数字，下同。
    df = df.loc[:, ['书籍ID', '书名']]
    df2 = pd.read_excel(enter2).astype('str')  # 帖子信息
    df2 = df2.loc[:, ['帖子ID', '标题', '正文', '关联书籍']]

    df3 = df2.loc[df2['关联书籍'] != 'nan'].copy()  # 取非空的行
    df3['书名'] = df3['关联书籍'].str.split(';')  # 把帖子关联的书籍从字符串拆分成列表
    df4 = df3.explode('书名').astype(str)  # 把帖子关联的书籍一列转多列行
    df5 = pd.merge(df, df4, on=['书名'], how='left').reset_index().astype('str')  # 合并书籍表和分裂后的表，每行都是帖子——书名——ID

    df6 = df5.groupby('帖子ID').agg({'书籍ID': lambda x: ','.join(x)}).reset_index()  # 把书籍ID按帖子ID进行聚合。
    df7 = pd.merge(df2, df6, on='帖子ID')  # 合并表格
    df7.to_excel('.\\enter\\书籍信息.xlsx')


def ownership(edi_jian: str, edi_fan: str, ku_fan: str, yi: str) -> Union[str, dict]:  # 判断书名部分字符的归属
    """
    我写的这串代码目的是什么?
    找到名称重叠,或一名多译的轻情况.通过同样的中文名称或者相似的(词组重合度70%视作相同)
    """

    temp = []
    try:
        # '把数据都存入参数列表中,把列表的值两两匹配'
        argument_list_pre = [edi_jian, edi_fan, ku_fan, yi]
        # '把存入的参数做去重,节约后续运行成本,因为数据来源不同，会出现一些特例。如果是直接继承Dataframe数据的话，传入的参数是list，'
        # 'pd.notna不能排除掉空值。所以需要新增判断 ”！=“ 否则就会出现没有排除空值，输出带空值的情况，如果是打开本地表格，那么数据格式是str'

        argument_list = []
        for i in argument_list_pre:
            if i not in argument_list and pd.notna(i) and i != '':
                argument_list.append(i)

        # '特例判断,去重只有1个值,那么最后输出它本身'
        if len(argument_list) == 1:
            dic = dict()
            dic[argument_list[0]] = argument_list[0]
            return dic

        # '两个弹出的值都需要做一次判断,判断是否有引号,如果有引号,用引号拆分为表格,没有引号,则赋值成为列表.'
        for l1_index in range(len(argument_list)):
            if pd.notna(argument_list[l1_index]):
                if ';' in argument_list[l1_index]:
                    target1 = argument_list[l1_index].split(';')
                else:
                    target1 = [argument_list[l1_index]]

                for l2_index in range(l1_index, len(argument_list)):
                    if pd.notna(argument_list[l2_index]):
                        if ';' in argument_list[l2_index]:
                            target2 = argument_list[l2_index].split(';')
                        else:
                            target2 = [argument_list[l2_index]]

                        # '当target2确定之后,调用switch函数,为temp生成数据'
                        switch_list = switch(target1, target2, temp)
                        # '用lis生成的字符串调用guishu_temp函数,生成最后一个版本的temp'
                        ownership_duplicate(switch_list, temp)

        return ''.join(";".join('%s' % ids for ids in temp))  # 把内容转成字符串.节省空间.
    except:
        return {}


def random_choice_list(data_adv: pd.DataFrame, num=100):
    """输入第一个参数，需要抽样的数据集Dataframe。输入第二个参数，需要将数据划分成几份，默认为100"""

    data_adv['抽样'] = data_adv.apply(lambda x: random_int(num), axis=1)

    df_c1 = data_adv[(data_adv['抽样'] == random_int(num)) |
                     (data_adv['抽样'] == random_int(num)) |
                     (data_adv['抽样'] == random_int(num))]

    df_c1.to_excel('.\\input\\抽样书名系列1.xlsx', index=False)
    print('抽样已输出')


def data_sampling(sample, total, num):
    # '保留关键列，节约输出内容页面'
    sample['抽样'] = sample.apply(lambda x: random_int(total))
    sample = sample.loc[:,
             ['isbn', '书籍名称（库）', '书籍名称（EDI）', '名称分词-库（繁）', '名称分词-EDI（繁）', '书名', '抽样']]
    random_choice_list(sample, num)
    sample.to_excel('.\\input\\抽样数据.xlsx')


def filter_dict():
    with open('替换.txt', 'r', encoding='utf-8') as f:  # 删除书籍名文档中的脏数据。
        replace_list = [line.strip() for line in f]

    # 处理书籍名
    with open('书籍名.txt', 'r', encoding='utf-8') as f:
        book_list = [line.strip() for line in f if line.strip() not in replace_list]
    with open('书籍名.txt', 'w', encoding='utf-8') as f:
        f.write('\n'.join(book_list))

    # 处理作者名
    with open('作者名.txt', 'r', encoding='utf-8') as f:
        author_list = list(set([line.strip() for line in f]))
    with open('作者名.txt', 'w', encoding='utf-8') as f:
        f.write('\n'.join(author_list))

    # 处理屏蔽关键词
    with open('屏蔽关键词.txt', 'r', encoding='utf-8') as f:
        block_list = [line.strip() for line in f]
    with open('屏蔽关键词.txt', 'w', encoding='utf-8') as f:
        f.write('\n'.join(block_list))


def label_database(enter_: pd.DataFrame, input_):
    def check(element: pd.DataFrame):  #
        res = []
        for i in eval(element):
            if i in label_list.keys() and label_list[i]:
                res.extend(label_list[i])
            else:
                return res
        return list(set(res))

    """对标签库进行优化，把书名和对应的标签组合起来生成新的字典。再把字典重新作用于标签库，对符合条件的空白数据做补充。"""
    label_list = dict()  # 储存字典

    label_house_ = pd.read_excel(enter_)
    name, label = label_house_.loc[:, ['书名', '个推标签']].apply(lambda x: x.str.split(';')).T.values.tolist()
    for k, v in zip(name, label):
        if v != 'nan' and pd.notna(v):
            for book_name_list in k:
                for book_name in eval(book_name_list):
                    if book_name not in label_list.keys():
                        label_list[book_name] = v
                    else:
                        if ''.join(v) in label_list[book_name]:
                            pass
                        else:
                            label_list[book_name].append(''.join(v))
    # 用已有数据重新遍历df列表，用已有数据对原表补充。
    label_house_['标签汇总'] = label_house_.apply(lambda x: check(x['书名']), axis=1)
    label_house_.to_excel(input_, index=False)
    label_list = {k: str(v) for k, v in label_list.items()}
    df = pd.DataFrame.from_dict(label_list, orient='index', columns=['标签'])
    df = df.rename_axis('关键词')
    # df.columns = ['关键词', '标签']
    df.to_excel('.\\input\\关键词—标签.xlsx')


def book_label(enter_label, enter_book, input_):
    """"""
    trans_finale = pd.read_excel('.\\input\\转换结束.xlsx')
    label = pd.read_excel(enter_label)  # 标签表
    book_infor = pd.read_excel(enter_book)  # 书籍表
    # 提取有效信息和保证列唯一，避免合并后因为有重复列出现x，y导致报错。
    book_infor = book_infor.loc[:, ['书名', 'isbn', '书籍分类标签', '帖子ID', '书籍ID']].rename(
        columns={'书名': '书籍名'})
    book_label_table = pd.merge(label, book_infor, on='书籍分类标签', how='inner')
    book_label_table = pd.merge(book_label_table, trans_finale, on='isbn', how='right'). \
        rename(columns={'兴趣标签小类': '个推标签'})
    book_label_table = book_label_table.loc[:, ['isbn', '书名', '书籍名', '书籍名称（库）',
                                                '书籍名称（EDI）', '译名', '作者', '书籍分类标签', '个推标签', '帖子ID', '书籍ID']]
    book_label_table.to_excel(input_)


def post_label(enter_post_table, book_label_table, keyword_label_table, input_):
    """
    输入三个参数为贴子文档，书籍标签库，关键词标签表，比对帖子包含的三个信息，书籍ID，标题，正文
    先比对书籍ID是否存在标签库中，如果存在，返回对应的标签。对标题进行分词，看是否出现标签库中的词汇，如果存在，返回对应标签
    对正文进行分词，看是否出现标签库中的词汇，如果存在，返回对应标签。以上任意一步产生了标签，则跳过。如果同时有多个标签，则返回前3个。
    """
    def check(book, check_table, mod=0):
        res = []
        if mod == 0:

            if ',' in book:
                list_temp = map(int, book.split(','))
            else:
                list_temp = map(int, [book])
            for num in list_temp:
                if num in list(check_table.keys()):
                    res.extend(check_table[num])
            res_num = list(collections.Counter(res).items())
            res_num = sorted(res_num, key=lambda x: x[1], reverse=True)  # 按值倒序排列
            return dict(res_num[:3])
        else:
            try:
                for i in book:
                    book_name = func.extract(i, func.books_name1, 1)
                    for s in book_name:
                        print(s, check_table[s])
                        res.extend(eval(check_table[s]))
                    if res:
                        break
                res_num = list(collections.Counter(res).items())
                res_num = sorted(res_num, key=lambda x: x[1], reverse=True)
            except:
                return dict()

            return dict(res_num[:3])  # 取出现数量最大的三个值

    post_table = pd.read_excel(enter_post_table).copy()  # 把数据划分成为两部分
    post_book_is = post_table.loc[post_table['书籍ID'].notna()].copy()  # 有关联书
    post_book_not = post_table.loc[post_table['书籍ID'].isna()].copy()  # 没关联书

    # 有关联书的样本处理，比对标签表是否包含该ID。
    number = pd.read_excel(book_label_table)
    number_table = number.loc[:, ['书籍ID', '标签汇总']].set_index('书籍ID')['标签汇总'].to_dict()  # 标签表
    number_table = {int(k): eval(v) for k, v in number_table.items() if pd.notna(k) and pd.notna(v)}  #
    post_book_is['标签汇总'] = post_book_is.apply(
        lambda x: check(x['书籍ID'], number_table), axis=1)

    post_book_is_not = post_book_is.loc[post_book_is['标签汇总'] == {}].copy()  # 有关联书但无法匹配到书对应标签的,需要再过一次字符标签
    post_book_is = post_book_is.loc[post_book_is['标签汇总'] != {}].copy()

    # 没关联书的样本处理，调用结巴分词对标题和正文进行分词。
    keyword_table = pd.read_excel(keyword_label_table).set_index('关键词')['标签'].to_dict()

    post_book_not['标签汇总'] = post_book_not.apply(
        lambda x: check([x['标题'], x['正文']], keyword_table, 1), axis=1)

    post_book_is_not['标签汇总'] = post_book_is_not.apply(
        lambda x: check([x['标题'], x['正文']], keyword_table, 1), axis=1)

    post_accomplish = pd.concat([post_book_is, post_book_not, post_book_is_not], axis=0)

    post_accomplish.to_excel(input_)


class Star():
    def __init__(self, df):
        self.trans_finale = None
        self.df_merge = None
        self.summary = None
        self.title = None
        self.df = None
        self.author_name1 = None
        self.books_name1 = None
        self.edi_data = df

    def collate_data(self, enter_, input_):  # 整理数据，根据自定义词典分词
        # '文档数据处理'
        with open('书籍名.txt', 'r', encoding='utf-8') as books_name:
            self.books_name1 = [i.replace('\n', '') for i in books_name]

        with open('系列名.txt', 'r', encoding='utf-8') as author_name:
            self.author_name1 = [i.replace('\n', '') for i in author_name]
        """两个参数是，输入文档地址，书籍整理输出地址，"""
        # 去除完成人工校对的数据，减少计算成本。
        finished_data = pd.read_excel(enter_)
        finished_data = finished_data.loc[:, ['isbn']]
        finished_data['isbn'] = finished_data['isbn'].astype(str)
        # 合并后生成一个归属标签，如果数据在两个列表中同时存在，会标记为’both‘.
        original_data = pd.merge(self.edi_data, finished_data, on='isbn', how='outer', indicator=True)
        original_data = original_data.loc[original_data['_merge'] != 'both']

        jieba.load_userdict('书籍名.txt')
        jieba.setLogLevel(jieba.logging.INFO)

        # '把数据按照给定的数据来源进行分词'
        original_data['名称分词-库（简）'] = original_data.apply(
            lambda x: func.extract(x['书籍名称（库）'], self.books_name1), axis=1)

        original_data['名称分词-库（简）'] = original_data['名称分词-库（简）'].to_frame().replace(func.jianfan_dic['简体'])
        # '将分词的结果转换成繁体,数据来源是数据集的结果转简体,再转繁体,然后合并到同一列.'
        original_data['名称分词-库（繁）'] = original_data.apply(
            lambda x: func.extract(x['书籍名称（库）'], self.books_name1), axis=1)

        # '流程同上'
        original_data['译名分词'] = original_data.apply(
            lambda x: func.extract(x['译名'], self.books_name1), axis=1)
        original_data['名称分词-EDI（繁）'] = original_data.apply(
            lambda x: func.extract(x['书籍名称（EDI）'], self.books_name1), axis=1)

        original_data['名称分词-EDI（简）'] = original_data['名称分词-EDI（繁）'].to_frame().replace(
            func.jianfan_dic['简体'])

        # '用上文的词典生成包含所有系列名词的自定义列表'
        jieba.load_userdict('系列名.txt')
        jieba.setLogLevel(jieba.logging.INFO)

        # '单纯用译名的时候需要去掉一些无效字段'
        original_data['译名'] = original_data['译名'].apply(
            lambda x: re.sub(books_str_replace, '', str(x)).strip() if pd.isna(x) is False else "")
        original_data['译名'] = original_data['译名'].apply(
            lambda x: re.sub(func.books_jishu, '', str(x)).strip() if pd.isna(x) is False else '')

        '从分好的词汇中提取系列词'
        original_data['书·系列'] = original_data.apply(
            lambda x: func.extract(x['名称分词-EDI（简）'], self.author_name1), axis=1)
        original_data.to_excel(input_, index=False)

    def title_aggregation(self, enter_, input_):  # 根据系列名词典为书籍生成系列名
        self.df = pd.read_excel(enter_)
        self.df['书名'] = self.df.apply(lambda x: ownership(
            x['名称分词-EDI（简）'], x['名称分词-EDI（繁）'], x['名称分词-库（繁）'], x['译名分词']), axis=1)

        self.df.to_excel(input_, index=False)

    def data_summary(self, enter_, input_):  # 把书名系列的内容转成
        self.summary = pd.read_excel(enter_)
        self.summary['书名'] = self.summary['书名'].str.split(';')  # 把每行的字典取出来组成列表,为了下一步使用explode
        self.summary = self.summary.explode('书名')  # explode每个单元格的值按长度转成行
        data_summary = self.summary.loc[:, ['isbn', '书名']]  # 减少无效信息
        data_summary = data_summary.reset_index(drop=True)  # 因为炸裂后的数据会出现重复索引的情况.所以在这里重置索引.

        l1 = []
        l2 = []

        # '经过上边的df分裂,值已经变成了字符串,所以在这里,需要转换一次格式.把字符串转成字典'
        for st in data_summary['书名']:  # 字典转两列
            try:
                i = eval(st)

                l1.append(list(i.keys()))
                l2.append(list(i.values()))
            except:
                l1.append([])
                l2.append([])

        # '把前边的列表转成列表'
        l1 = [''.join(i) for i in l1]
        l2 = [''.join(i) for i in l2]

        # '把列表转为Series'
        col1 = pd.Series(l1, name='书名键')
        col2 = pd.Series(l2, name='书名值')

        # '合并数据'
        data1 = pd.concat([data_summary, col1, col2], axis=1)

        # '把键值的的字符统一,去掉字符中的符号以及统一大小写'
        data1['格式化书名键'] = data1.apply(lambda x: set_dict(x['书名键']), axis=1)
        data1['格式化书名值'] = data1.apply(lambda x: set_dict(x['书名值']), axis=1)

        # '不要去重,否则书名信息不全.'
        data1 = data1.loc[data1['书名键'] != '']  # 去除空值
        data1.reset_index(drop=True, inplace=True)
        data1.to_excel(input_, index=False)

    def data_merge(self, enter):
        self.df_merge = pd.read_excel(enter)
        # '以下两列输出的结果都是{索引：值} 把原数据的键值转化成列表。
        lis_dic1 = self.df_merge['格式化书名键'].to_dict()
        lis_dic2 = self.df_merge['格式化书名值'].to_dict()
        li3_dic3 = self.df_merge['isbn'].to_dict()
        target_list = [lis_dic1, lis_dic2]

        # '调用dict_cnt函数后,生成的键值对,键是书名一部分,值是出现过键的索引，将键值对和isbn同时传入函数，记录isbn对应的键值'
        df = pd.DataFrame.from_dict(dict_cnt(target_list, li3_dic3), orient='index')
        df.to_excel('.\\input\\书名索引.xlsx', index=False)  # 中途转出

        df = pd.read_excel('.\\input\\书名索引.xlsx').rename(columns={0: '索引列'})  # 重命名列
        data = pd.read_excel('.\\input\\书名整理.xlsx').loc[:, ['isbn', '书名键', '书名值']]
        data['书名整理'] = df.apply(lambda x: dict_index(x['索引列'], data), axis=1)

        dict_total_list = {}
        df = data['书名整理'].dropna().tolist()

        dict_trans_list(df, dict_total_list)
        list_df = pd.DataFrame.from_dict(dict_total_list, orient='index')
        list_df = list_df.reset_index().rename(columns={'index': 'isbn', 0: '书名'})
        res_finale = pd.merge(list_df, pd.read_excel('无标题.xlsx'), on=['isbn'])  # 合并元素
        list_df.to_excel('.\\input\\书名解析.xlsx', index=False)
        res_finale = res_finale.loc[:, ['isbn', '书名', '书籍名称（库）', '书籍名称（EDI）', '译名', '作者', '更新说明']]
        res_finale.to_excel('.\\input\\转换结束.xlsx', index=False)


if __name__ == '__main__':
    step0 = time.time()
    books_str_replace = '\(.*?\)|\[.*?]|【.*?】|（.*?）|（.*?）|完$|復刻版|漫画版|〈.*?〉| \((.*?)\)|\〔.*?〕|/.*|\(.*?）|完全版|愛藏版'
    lis = list()  # 作为储存switch函数对照数据的列表。

    edi_data = pd.read_excel('无标题.xlsx')
    run = Star(edi_data)
    filter_dict()  # 整理字典信息
    # 整理数据，根据自定义词典分词
    run.collate_data('.\\enter\\完成分词.xlsx', '.\\input\\书名系列整理.xlsx')  # 整理数据，分词
    step1 = time.time()
    print(f'step1已执行{step1-step0}s')

    run.title_aggregation('.\\input\\书名系列整理.xlsx', '.\\input\\书名系列.xlsx')  # 根据系列名词典为书籍打上系列名标签
    step2 = time.time()
    print(f'step2已执行{step2-step1}s')

    run.data_summary('.\\input\\书名系列.xlsx', '.\\input\\书名整理.xlsx')  # 对分词后的数据统一格式，减少因为格式不统一导致的无法匹配
    step3 = time.time()
    print(f'step3已执行{step3-step2}s')

    run.data_merge('.\\input\\书名整理.xlsx')  # 根据格式化后的书名相同部分在数据内互相匹配，合并。
    step4 = time.time()
    print(f'step4已执行{step4-step3}s')

    book_label('.\\enter\\书籍分类对应标签.xlsx', '.\\enter\\帖子书籍信息.xlsx',
               '.\\input\\书籍标签库.xlsx')  # 把帖子和书籍合并，根据映射生成标签。
    step5 = time.time()
    print(f'step5已执行{step5 - step4}s')

    label_database('.\\input\\书籍标签库.xlsx', '.\\input\\书籍标签库_new.xlsx')  # 标签库更新，分成关联书籍标签库和关键词标签库
    step6 = time.time()
    print(f'step6已执行{step6 - step5}s')

    jieba.load_userdict('书籍名.txt')
    jieba.setLogLevel(jieba.logging.INFO)

    post_label('.\\enter\\贴子信息.xlsx', '.\\input\\书籍标签库_new.xlsx',
               '.\\input\\关键词—标签.xlsx', '.\\input\\标签.xlsx')  # 用两个标签库为帖子数据匹配标签
    step7 = time.time()
    print(f'step7已执行{step7 - step6}s')

    # print(time.time() - step0)
    # sampling = pd.read_excel('.\\input\\书名系列1.xlsx')  # 对文档进行抽样
    # data_sampling(sampling, 100, 10)
