from typing import Union

import pandas as pd
import re
import random
import func
import difflib
import jieba

def dict_trans_list(enter_argument: list, input_list: dict) -> dict:
		"""
		传入列表和储存输出值的外部列表，传入的参数格式是[{isbn:[book_name]}]，从参数中逐个读取字典，把字典存入输出列表中，每次便利输出的值，比对
		该字典的键是否已经存在与输出列表的键中，如果存在，把遍历到的键值对和已存在字典的键值对进行去重，合并。最后把输出键值对的值转成str格式，转成
		Dataframe的时候不会因为列表长度太长，而导致表格数据过多。
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

def dict_cnt(dict0_total_list: list, dict_index: dict) -> dict:  # 索引匹配 传入一个列表。遍历列表内的值。即可实现传入多个列表的目的。
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

		'这里相当于是一次遍历,把出现这个书名的isbn都放在一起.'
		for dict_list_total in dict0_total_list:  # 从输入值拆分参数,只要有一个列表就行.不用写死传入多少个参数
				for index_, word in dict_list_total.items():  # 对参数进行遍历,输出是{ISBN(int格式) : 字典(str)}
						cnt += 1
						if type(word) == str:  # 如果是从前边继承来的值,那么这里应该直接就是dict.不用转格式,所以要写if...else
								dict_v = eval(word)
						else:
								dict_v = word
						'参数k是格式值 参数v是格式后的值'
						for k, v in dict_v.items():  # 对每行参数字典化后键值在输出的字典中做匹配.
								'如果要写正则,可以加在这里.但似乎加了模糊匹配,每个值都要遍历一次列表.那就是n^2'
								# print(dict_v, '\ns', dict_total)
								# 如果值在输出的字典中.说明已经有这个信息,所以往列表里加上这个索引. 因为有源列表有两个元素,所以要遍历两次.
								if v in dict_total.keys():
										dict_total[v].append(dict_index[index_])
								else:  # 如果不然，键赋值列表。
										dict_total[v] = [dict_index[index_]]

								# diff = difflib.get_close_matches(v,list(dict_total.keys()))  # 这个是模糊搜索
								# if diff != []:
								# 		for s in diff:
								# 				if s in dict_total.keys():  # 如果值在输出的字典中.说明已经有这个信息,所以往列表里加上这个索引. 因为有源列表有两个元素,所以要遍历两次.
								# 						dict_total[v].extend(dict_total[s])
								# 						dict_total[v] = list(set(dict_total[v]))
								# 				else:  # 如果不然，键赋值列表。
								# 						dict_total[s] = [index_]

		'格式转换,列表数据转成字符串,在输出表格时结果就只有两列.而不是数十列.'
		for k, v in dict_total.items():
				dict_total[k] = str(list(set(v)))
		return dict_total

def dict_index(lis: Union[list, str], data_dict: pd.DataFrame) -> dict:  # 输入的是要找索引的文本列表。以及列表对应索引的值。
		"""输入字符，转为列表。以列表的值通过索引获取对应两个信息"""
		index_list = eval(lis)
		res = []
		dic = {}
		for index_ in index_list:
				'这里是不是还应该把之前的数据根据isbn聚合在一起?'
				res.extend(list((data_dict.loc[data_dict['isbn'] == index_])['书名键'].values))
				# print(cnt_num,index_, list((data_dict.loc[data_dict['isbn'] == index_])['书名键'].values))
				res.extend(list((data_dict.loc[data_dict['isbn'] == index_])['书名值'].values))

		for index_ in index_list:
				dic[index_] = list(set(res))

		return dic

cnt_num = 0
def jf(j: Union[str, list], f: Union[str, list], lis1: list) -> Union[str, list]:
		"""
		本来是为了应对简繁的.但后来发现可以用于为两个列表进行分裂匹配.输入的都是字符串
		把参数a转成列表,把参数b也转成列表.
		20230110 因为之前没有往函数中定义储存的地方。所以当出现带分号的数据时，每次调用返回的值都没办法传递给下一个
		"""
		'把输入的两个参数转变成列表,注意转换后的格式,可能出现通过一条逻辑转成列表.'
		'所以要针对能想到的可能导致转换失败的情况编写判断逻辑和转换逻辑'
		# print(j, type(j), f, type(f))
		try:
				'如果分隔符存在字符串中.以分隔符拆分成列表,否则直接把参数赋值到列表中'
				for n in range(len(j)):
						'从n的位置开始向参数b分割成的列表遍历'
						for b_n in range(n, len(f)):
								'因为是把这一行的值都进行关联,所以是不等于,且不为空.'
								if j[n] != f[b_n] and f[b_n] != '' and j[n] not in lis1:
										dic = dict()
										dic[j[n]] = f[b_n]
										if dic not in lis1:
											lis1.append(dic)

		except:
				return []
		'%s 是排除了可能出现的纯数字结果导致报错.'
		return ";".join(['%s' % id1 for id1 in lis1])

def tihuan(s: str, r: str) -> str:  # 替换
		if pd.isna(s):
				return ''
		else:
				return re.sub(r, '', s)

def random_int(n: int) -> int:
		"""
		输入一个数字，需要将数据划分成几份，则为数字为几，默认为100
		"""
		return random.randint(1, n)

def set_dict(name: str):
		"""输出的是输入参数的大写版本.用来将数据统一"""
		dic = {}
		g_name = func.daxiaoxie(name)
		dic[name] = g_name
		return dic

def ownership_duplicate(a: [str, list], b: list) -> list:  # ownership 归属英文 + duplicate 复制英文
		"""
		直接把字符化的字典赋值到列表时,系统会自动补充斜杠
		输入的参数只会有无分号的字符串,单独的字典字符串,一个空列表,带分号的字符串
		注意越限，和特例，避免报错，或者对报错应该要有应对措施，不要让它直接跳出运转。
		"""
		try:

				if a == [''] or pd.isna(a) or a == '':
						return b

				'把输入的参数转成列表，在函数外的前一步已经将输入的内容转成字符串了。'
				'这里相当于是把字符串重新转为字典'
				team_lis = a.split(';') if ';' in a else [eval(a)]

				'从lis中输出值，逐个比对是否存在传递结果的外部列表中。'
				for i1 in team_lis:
						i_dict = eval(i1)
						if i_dict not in b and pd.notna(i_dict):
								b.append(i_dict)  # 两个列表合并
				return b
		except:
				pass

def ownership(edi_jian: str, edi_fan: str, ku_fan: str, yi: str) -> Union[str, dict]:  # 判断书名归属.ownership 归属的英文
		"""
		我写的这串代码目的是什么?
		找到名称重叠,或一名多译的轻情况.通过同样的中文名称或者相似的(词组重合度70%视作相同)
		不考虑相似的了.如果只考虑相似的结果会因为A像B B像C 出现A像C 要这么写,想要实现效果,要写很多判断,就目前的我来说,
		我不具备这样的思维和能力.就目前的情况来说,机器学习,或者AI可以实现一部分.如果休息的时候,可以考虑做一下.

		有几种情况
		1.分号是否在单元格内 ,在单元格内,说明最少有两个词.这种要怎么处理?  func.quchong 是去掉符号,只看文字内容相不相同
		2.简体和繁体是否相同
		3.两个单元格因为符号不统一导致结果判断结果不同.

		特例
		有一个特例是x和X和×使用不规范,这种问题要加一个判断.判断两者不相同是否是因为三个×的款式不同.
		简繁译为空
		同一本书，但由于符号不同未被聚合。

		还有其他情况吗?

		第一步是什么? 判断是否为空.如果非空,进入下一步判断,如果译名为空,跳过空值.其他的照常判断.如果简体繁体都为空,则三个值都是空,输出空.
		第二步是什么? 把单个词组的数据处理完毕.以分号为判断依据.分号如果不在列表中,说明是单个词汇.可以直接进行处理. 处理指→?
		第三步是什么? 处理第二步遗留下来的数据,按照分号划分为列表.

		好像可以优化,把四个值当作是一个列表,只需要拆分列表的值,在列表中进行比对,似乎就可以了.不需要加一些额外的判断.最后输出还能统一
		已经优化,但键值相反的两字典无法去重.这可能会多出来几个无效数据.
		"""
		temp = []
		try:
				'把数据都存入参数列表中,把列表的值两两匹配'
				argument_list_pre = [edi_jian, edi_fan, ku_fan, yi]
				'把存入的参数做去重,节约后续运行成本,因为数据来源不同，会出现一些特例。如果是直接继承Dataframe数据的话，传入的参数是list，'
				'notana不能排除掉空值。所以需要新增判断 ”！=“ 否则就会出现没有排除空值，输出带空值的情况，如果是打开本地表格，那么数据格式是str'
				'以上只是猜测，加上！= ’‘ 之后，输出已经正常。而之前直接读取表格的话，不用等于空字符也能正常输出。'
				argument_list = [i23 for i23 in list(set(argument_list_pre)) if pd.notna(i23) and i23 != '']
				# print(argument_list)
				'特例判断,去重只有1个值,那么最后输出它本身'
				if len(argument_list) == 1:
						dic = dict()
						dic[argument_list[0]] = argument_list[0]
						return dic

				'按照 12 13 14 23 24 34 这样的下标进行遍历'
				'两个弹出的值都需要做一次判断,判断是否有引号,如果有引号,用引号拆分为表格,没有引号,则赋值成为列表.'
				for l1_index in range(len(argument_list)):
						if pd.notna(argument_list[l1_index]):
								if ';' in argument_list[l1_index]:
										target1 = argument_list[l1_index].split(';')
								else:
										target1 = [argument_list[l1_index]]

								for l2_index in range(l1_index, len(argument_list)):
										# print('l1', l1_index,argument_list[l1_index], 'l2', l2_index,argument_list[l2_index])
										if pd.notna(argument_list[l2_index]):
												if ';' in argument_list[l2_index]:
														target2 = argument_list[l2_index].split(';')
												else:
														target2 = [argument_list[l2_index]]
												# print(target1,target2)
												'当target2确定之后,调用jf函数,为temp生成数据'
												print('origin', target1, target2)
												lis = jf(target1, target2, temp)
												'用lis生成的字符串调用guishu_temp函数,生成最后一个版本的temp'
												ownership_duplicate(lis, temp)

				return ''.join(";".join('%s' % ids for ids in temp))  # 把内容转成字符串.节省空间.
		except:
				return {}


def random_choice_list(data_adv: pd.DataFrame, num=100):
		"""
		输入第一个参数，需要抽样的数据集Dataframe。
		输入第二个参数，需要将数据划分成几份，默认为100
		"""
		data_adv['抽样'] = data_adv.apply(lambda x: random_int(num), axis=1)

		df_c1 = data_adv[(data_adv['抽样'] == random_int(num)) |
										 (data_adv['抽样'] == random_int(num)) |
										 (data_adv['抽样'] == random_int(num))]

		df_c1.to_excel('抽样书名系列1.xlsx', index=False)
		print('抽样已输出')


books_tihuan = '\(.*?\)|\[.*?]|【.*?】|（.*?）|（.*?）|完$|復刻版|漫画版|〈.*?〉| \((.*?)\)|\〔.*?〕|/.*|\(.*?）|完全版|愛藏版'

lis = list()
dic = dict()

'下边使用的无标题文档，其实是由我自己合成的表格。EDI里导出的只有他的商品名，从书籍库中导出的他的上架名'
edi_data = pd.read_excel('无标题.xlsx')
edi_edi_data = edi_data.loc[:, ['isbn', '书籍名称（库）', '书籍名称（EDI）']]

jieba.load_userdict('书籍名.txt')
jieba.setLogLevel(jieba.logging.INFO)

'文档数据处理'
books_name = open('书籍名.txt', 'r',encoding='utf-8')
books_name1 = [i.replace('\n', '') for i in books_name]

'把数据按照给定的数据来源进行分词'
edi_data['名称分词-库（简）'] = edi_data.apply(lambda x: func.tiquzuozhe(x['书籍名称（库）'], books_name1),axis=1)
edi_data['名称分词-库（简）'] = edi_data['名称分词-库（简）'].to_frame().replace(func.jianfan_dic['简体'])

'将分词的结果转换成繁体,数据来源是数据集的结果转简体,再转繁体,然后合并到同一列.'
edi_data['名称分词-库（繁）'] = edi_data.apply(lambda x: func.tiquzuozhe(x['书籍名称（库）'], books_name1),axis=1)

'流程同上'
edi_data['译名分词'] = edi_data.apply(lambda x: func.tiquzuozhe(x['译名'], books_name1), axis= 1)
edi_data['名称分词-EDI（繁）'] = edi_data.apply(lambda x: func.tiquzuozhe(x['书籍名称（EDI）'], books_name1), axis=1)
edi_data['名称分词-EDI（简）'] = edi_data['名称分词-EDI（繁）'].to_frame().replace(func.jianfan_dic['简体'])

'切换自定义词典(我猜测如果之前的自定义词典应该已经不用了.转而使用下边的系列名文档)'
jieba.load_userdict('系列名.txt')
jieba.setLogLevel(jieba.logging.INFO)

'用上文的词典生成包含所有系列名词的自定义列表'
anthors_name = open('系列名.txt', 'r', encoding='utf-8')
anthors_name1 = [i.replace('\n', '') for i in anthors_name]

'单纯用译名的时候需要去掉一些无效字段'
edi_data['译名'] = edi_data['译名'].apply(
		lambda x: re.sub(books_tihuan, '', str(x)).strip() if pd.isna(x) is False else "")
edi_data['译名'] = edi_data['译名'].apply(
		lambda x: re.sub(func.books_jishu, '', str(x)).strip() if pd.isna(x) is False else '')

'从分好的词汇中提取系列词'
edi_data['书·系列'] = edi_data.apply(lambda x: func.tiquzuozhe(x['名称分词-EDI（简）'], anthors_name1), axis=1)
edi_data.to_excel('书名系列整理.xlsx', index=False)
'=======================================整理抽样========================================'

'把前文产生的数据赋值给df'
#df = edi_data
df = pd.read_excel('书名系列整理.xlsx')

' 以简体为主。其他繁体和英文、日文，全部当作是简体的附属 '
' 先吧简繁做一次对应。如果没有分号。而且简繁不等。 '
' 有些书名会包含日语和繁体.这部分数据要取出来才行.应该有好几种判断才能构达到目的.'

'20221128 程序可运转,但少了一万多数据.译名分词和EDI分词的数据空了.'
'20221129 问题已解决,因为func.guishu函数中,a,b参数错了.修改函数的时候a参数被去掉了.'
'取出两列数据,把数据用笛卡尔乘积,找到对应关系.'

df['书名'] = df.apply(lambda x: ownership(
		x['名称分词-EDI（简）'], x['名称分词-EDI（繁）'], x['名称分词-库（繁）'], x['译名分词']), axis=1)

'测试数据，用少量数据试跑程序，看输出如何'
df['书名'] = df.apply(lambda x: jf(
		x['名称分词-EDI（简）'], x['译名分词']), axis=1)
df.to_excel('书名系列.xlsx', index=False)

'========================================抽样数据========================================'
data_adv = pd.read_excel('书名系列_text.xlsx')
'保留关键列，节约输出内容页面'
data_adv = data_adv.loc[:,
           ['isbn', '书籍名称（库）', '书籍名称（EDI）', '名称分词-库（繁）', '名称分词-EDI（繁）', '书名', '抽样']]
random_choice_list(data_adv, 10)

'问题在于,有些名字不是在一个格子里,需要自己找.要写函数去搜索.'
'进度，解压的数据已经答题符合预期。。甲骨文丛书这里，少了后半截。还有空值没去掉。重复值也没去掉。'
'去重和空置,应该还是可以在guishu的函数for循环时eval转为字典然后去掉空和重复.'

'20221128 对1万8条信息(因为系列漫画,以及多次关联去重得到的结果) 抽了两次数据,分别是1%左右和3%左右. 1%,错误3条,3%错误19条'
'目前可见的错误是,(1) 冒号和破折号并用.在两个版本中一个用冒号,一个用破折号.(2) 简繁不同 (3)多余关联的数据,多余的数据可能是来自于括号内的.'
'或者是用作者比较出名的作品引流的.'
'func.quchong 可以把数据格式统一为大写,但这个东西应该在哪里用?应该怎么用?'

'========================================整理汇总========================================'
df = pd.read_excel('书名系列.xlsx')
df['书名'] = df['书名'].str.split(';')  # 把每行的字典取出来组成列表,为了下一步使用explode
df = df.explode('书名')  # explode每个单元格的值按长度转成行
data = df.loc[:, ['isbn', '书名']]  # 减少无效信息
data = data.reset_index(drop=True)  # 因为炸裂后的数据会出现重复索引的情况.所以在这里重置索引.

l1 = []
l2 = []

'经过上边的df分裂,值已经变成了字符串,所以在这里,需要转换一次格式.把字符串转成字典'
for st in data['书名']:  # 字典转两列
		try:
				i = eval(st)
				l1.append(list(i.keys()))
				l2.append(list(i.values()))
		except:
				l1.append([])
				l2.append([])

'把前边的列表转成列表'
l1 = [''.join(i) for i in l1]
l2 = [''.join(i) for i in l2]

'把列表转为Series'
col1 = pd.Series(l1, name='书名键')
col2 = pd.Series(l2, name='书名值')

'合并数据'
data1 = pd.concat([data, col1, col2], axis=1)

'把键值的的字符统一,去掉字符中的符号以及统一大小写'
data1['格式化书名键'] = data1.apply(lambda x: set_dict(x['书名键']), axis=1)
data1['格式化书名值'] = data1.apply(lambda x: set_dict(x['书名值']), axis=1)

# data1.drop_duplcaites(subset=[ '书名键', '书名值'], keep='first', inplace=True)   # '不要去重,否则书名信息不全.'

data1 = data1.loc[data1['书名键'] != '']  # 去除空值.是否有必要去除空值? 或者说前文的空值是否有必要
data1.reset_index(drop=True, inplace=True)
data1.to_excel('书名整理.xlsx', index=False)

'怎么把两列内容合并?'
'新建两列临时列。如果A临时列数据等于B临时列，或者在B临时列中且A列的占比大于66.7%（3个字有2个字相同）'
'符合条件的,视作B列为A列的延申.这只能11匹配.没办法做到聚合.聚合还要再多一个判断.`'

'========================================测试========================================'
'设置好索引列,不管我是用键值对中的哪个,都会出现重复问题,这也就意味着,可能会因为键重复而出现数据不全'
df = pd.read_excel('书名整理.xlsx')
'以下两列输出的结果都是{索引：值}'
lis_dic1 = df['格式化书名键'].to_dict()
lis_dic2 = df['格式化书名值'].to_dict()
li3_dic3 = df['isbn'].to_dict()
target_list = [lis_dic1, lis_dic2]

'调用dict_cnt函数后,生成的键值对,键是书名一部分,值是出现了这个部分的索引.'
df = pd.DataFrame.from_dict(dict_cnt(target_list, li3_dic3), orient='index')
df.to_excel('书名索引.xlsx', index=False)

df = pd.read_excel('书名索引.xlsx').rename(columns={0: '索引列'})
data = pd.read_excel('书名整理.xlsx').loc[:, ['isbn', '书名键', '书名值']]

data['书名整理'] = df.apply(lambda x: dict_index(x['索引列'], data), axis=1)

dict_total_list = {}
df = data['书名整理'].dropna().tolist()
lis = dict_trans_list(df, dict_total_list)

list_df = pd.DataFrame.from_dict(dict_total_list, orient='index')
list_df = list_df.reset_index().rename(columns={'index': 'isbn'})

# list_df.to_excel('1.xlsx', index=False)

res_finale = pd.merge(list_df, pd.read_excel('无标题.xlsx'), on=['isbn'])
list_df.to_excel('书名解析.xlsx', index=False)
res_finale.to_excel('1.xlsx', index=False)
