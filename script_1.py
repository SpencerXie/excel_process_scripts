from datetime import datetime

import pandas as pd
import traceback
import sys
from concurrent.futures import ProcessPoolExecutor


def merge_excel_files(input_dict, output_file, output_list):
    # 创建一个空的DataFrame来存储合并的数据
    merged_data = pd.DataFrame()

    # 遍历输入dict
    for file_name, sheet_info in input_dict.items():
        for sheet_name, columns in sheet_info.items():
            try:
                # 如果sheet_name为空字符串，使用None作为sheet_name的值
                if sheet_name == '':
                    sheet_name = 0
                # 读取Excel文件的特定工作表
                data = pd.read_excel(file_name, sheet_name=sheet_name)

                # 选择input_list指定的列
                if columns is not None and len(columns) > 0:
                    # 为输入数据添加一个空列，用于匹配输入为空的case
                    data[''] = ''
                    data = data[columns]
                if output_list is not None and len(output_list) > 0:
                    data.columns = output_list[:len(data.columns)]
                    # 检查并填充缺失的列
                    for col in output_list:
                        if col not in data.columns:
                            data[col] = None
                # 添加一列来存储文件名
                data.insert(0, 'SourceFile_Sheet', f'{file_name}_{sheet_name}')
                # 将数据添加到merged_data中
                merged_data = pd.concat([merged_data, data])
            except Exception as e:
                print(f"在处理文件 {file_name} 的 {sheet_name} 时发生错误: {e}")
                traceback.print_exc()  # 打印出错误的详细信息，包括错误发生的行号
                sys.exit(1)  # 终止程序，返回一个非零的退出状态，表示程序遇到了错误
    # 将合并的数据写入csv/excel文件
    save_data(output_file, merged_data)


def save_data(output_file, data):
    if output_file.endswith('.csv'):
        # 保存为CSV格式
        data.to_csv(output_file, index=False)
    elif output_file.endswith('.xlsx'):
        # 保存为Excel格式
        print("【", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), f"】 Excel数据长度为: {len(data)}")
        if len(data) > global_max_rows:
            save_large_data_to_excel(data, output_file)
        else:
            data.to_excel(output_file, index=False)
    else:
        print("Unsupported file format. Please use either CSV or Excel.")


def save_part_data(part_data, part_file):
    part_data.to_excel(part_file, index=False)


def save_large_data_to_excel(data, output_file):
    num_parts = (len(data) - 1) // global_max_rows + 1
    with ProcessPoolExecutor() as executor:
        futures = []
        for i in range(num_parts):
            part_data = data[i * global_max_rows:(i + 1) * global_max_rows]
            part_file = output_file.replace('.xlsx', f'_part{i + 1}.xlsx')
            future = executor.submit(save_part_data, part_data, part_file)
            futures.append(future)

        # 等待所有任务完成
        for future in futures:
            future.result()


def read_input_dict_from_excel(file_name, sheet_name=''):
    if sheet_name is None or sheet_name == '':
        sheet_name = 0
    data = pd.read_excel(file_name, sheet_name=sheet_name)
    data = data.fillna('')
    columns = data.columns.tolist()
    output_col_list = columns[2:]
    output_dict = {}
    # 使用iterrows()方法遍历每一行数据
    for index, row in data.iterrows():
        row.tolist()
        key_file, key_sheet = str(row[columns[0]]), row[columns[1]]
        if not key_file.endswith('.xlsx'):
            key_file += '.xlsx'
        sheet_columns = row.tolist()[2:]
        if key_file in output_dict:
            output_dict[key_file][key_sheet] = sheet_columns
        else:
            output_dict[key_file] = {key_sheet: sheet_columns}
    return output_dict, output_col_list


global_max_rows = 1048570

if __name__ == '__main__':
    '''
    1、sheet名为''时表示使用第一个sheet；
    2、列字段数组为空数组或者None时表示选择所有的字段；
    3、列字段为''表示占位空列。
    '''
    input_dict = {'file1.xlsx': {'': ['a1', 'a2', '', '名字']},
                  'file2.xlsx': {'Sheet1': ['b1', 'b2', 'b3', 'b4'], 'Sheet2': ['b6', 'b7', 'b8', 'b9']},
                  'file3.xlsx': {'Sheet1': ['c1', 'c2', 'c3', 'c4', 'c5']}
                  }
    # output_list 为空数组或者None时表示不修改输出列名
    output_list = ['out1', 'out2', 'out3', 'out4', '哈哈']
    # 可以选 scv 也可以选 xlsx 格式
    output_file = 'output1.csv'
    print("【", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), f"】 开始合并: {input_dict}")
    merge_excel_files(input_dict, output_file, output_list)
    print("【", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), f"】 {output_file}合并完成")

    input_dict = {'file4.xlsx': {'': []},
                  'file5.xlsx': {'Sheet1': [], 'Sheet2': []}}
    output_list = None
    output_file = 'output1.xlsx'
    print("【", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), f"】 开始合并: {input_dict}")
    merge_excel_files(input_dict, output_file, output_list)
    print("【", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), f"】 {output_file}合并完成")

    input_dict, output_list = read_input_dict_from_excel(file_name='input_dict.xlsx')
    output_file = 'output1.1.xlsx'
    print("【", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), f"】 开始合并: {input_dict}")
    merge_excel_files(input_dict, output_file, output_list)
    print("【", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), f"】 {output_file}合并完成")

