import pandas as pd
import re
import collections
import os


def number_filter_str(s: str):  # 读取映射表的数据，根据输入的序号转换成文字
	s = str(s)
	try:
		res = []
		try:
			lis = re.split(r'[,.，。]', s)
		except:
			lis = [s]

		for num in lis:
			try:
				res.append(target_dic['兴趣标签小类'][int(num)])
			except:
				pass
		return res
	except:
		return s


def filter_res_input(file: pd.DataFrame):
		dic_map = {num: name for num, name in enumerate(file, 1)}  # 把传入的参数编号。
		temp_lis = []  # 合并、储存传入参数的值
		dic = collections.defaultdict(int)  # 统计文本出现次数
		dic_index = {}  # 储存出现特定次数值的位置。

		for i in file:
				temp_lis.extend(i)

		for num, label in enumerate(temp_lis):
				dic[label] += 1

		for label, num in dic.items():
			if num in choice_num:
				for index_, list_ in dic_map.items():
					if label in list_:
						if label not in dic_index.keys():
							dic_index[label] = [index_]
						else:
							dic_index[label].append(index_)
		return [dict(dic), dic_index]


class CheckLabel:
	def __init__(self, a, b):
		self.file_index = a
		self.input_index = b
		self.dic_map = {}

		file_list = [name_excel for name_excel in os.listdir(file_index)]
		result_table = pd.DataFrame()

		for num, excel in enumerate(file_list, 1):
			solo_xlsx = pd.read_excel(
					f'{file_index}\\{excel}', sheet_name='Sheet1')
			self.dic_map[excel] = num
			result_table[f'{excel}'] = solo_xlsx.apply(lambda x: number_filter_str(x['标签编号']), axis=1)

		original_table = pd.read_excel(
			f'{file_index}\\{file_list[0]}', sheet_name='Sheet1').loc[:, ['帖子ID', '标题', '正文', '作者', '原文链接']]  # 需要保留原表的几个字段，注意这里的字段需要和表格内的字段保持一致
		result_table_new = result_table.apply(filter_res_input, axis=1).apply(pd.Series, index=['统计', '异常'])
		result_table = pd.concat([original_table, result_table, result_table_new], axis=1)
		# 会给与传入文件两级表头,文件名下方序号和异常中序号中一致.
		result_table.columns = pd.MultiIndex.from_tuples([(k, v) for k, v in (zip(result_table.columns,
		[self.dic_map[i] if i in self.dic_map.keys() else '' for i in result_table.columns]))], names=['列名', '序号'])

		# 因为有多级表头,输出文件的会多一行.手动删除即可.
		result_table.to_excel(f'{input_index}\\标签检验结果.xlsx')  # 输出文件的位置和输出的文件名


if __name__ == "__main__":
		label_map_file = '.\\备份\\标签映射表.xlsx'  # 标签映射表的位置,具体到表格地址
		file_index = '.\\标签检验\\'  # 文件地址,具体到文件夹
		input_index = '.\\'  # 文件存放在哪,如果不写路径默认放在当前py文件所在的文件夹

		choice_num = [1]  # 想看选择数是多少的，在列表添加数量即可。

		target_dic = pd.read_excel(label_map_file, sheet_name='Sheet1').set_index(['序号']).to_dict()
		run = CheckLabel(file_index, input_index)
