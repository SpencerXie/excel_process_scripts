import pandas as pd
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor


def read_file(file_name):
    if file_name.endswith('.csv'):
        # 读取CSV格式
        df = pd.read_csv(file_name)
    elif file_name.endswith('.xlsx'):
        # 读取Excel格式
        df = pd.read_excel(file_name)
    else:
        print("Unsupported file format. Please use either CSV or Excel.")
        df = pd.DataFrame()
    print("【", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "】 读取", file_name, "完成")
    return df


def merge_files(file1, key1, file2_name_list, key2, cols_to_copy, file2_collumn_date, special_len_list, output_file,
                new_col_names):
    # 读取两个文件
    df1 = pd.read_excel(file1)
    print("【", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "】 读取file1完成")
    # df_list = []  # 用于存储每个文件的DataFrame
    # for file_name in file2_name_list:
    #     df_temp = pd.read_excel(file_name)
    #     df_list.append(df_temp)
    #     print("【", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "】 读取", file_name, "完成")
    with ProcessPoolExecutor() as executor:
        df_list = list(executor.map(read_file, file2_name_list))

    # 使用pd.concat合并所有的DataFrame
    df2 = pd.concat(df_list)
    print("【", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "】 合并file2_list完成")

    # 检查new_col_names中的列名是否已经存在于df1中
    for col in new_col_names:
        if col in df1.columns:
            raise ValueError(f"Column name '{col}' already exists in {file1}")

    # 特殊逻辑处理：如果key2的长度在special_len_list中，那么复制这行并新增两行，新增行的key2字段分别为原来前半部分，后半部分
    for length in special_len_list:
        mask = df2[key2].str.len().eq(length)
        df_temp_left = df2[mask].copy()
        df_temp_left[key2] = df_temp_left[key2].str[:length // 2]
        df_temp_right = df2[mask].copy()
        df_temp_right[key2] = df_temp_right[key2].str[length // 2:]
        df2 = pd.concat([df2, df_temp_left, df_temp_right])
    # 使用merge函数合并两个dataframes
    df_merged = pd.merge(df1, df2, how='left', left_on=key1, right_on=key2)
    # 使用字典修改指定列名
    new_column_names = {}
    for col, new_col in zip(cols_to_copy, new_col_names):
        new_column_names[col] = new_col
    df_merged.rename(columns=new_column_names, inplace=True)
    df_merged = df_merged[df1.columns.tolist() + new_col_names]
    # 输出到新的文件中
    df_merged.to_excel(output_file, index=False)
    print("【", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "】输出到新的文件", output_file, "中完成")


if __name__ == '__main__':
    # freeze_support()
    file1_name = 'file11.xlsx'
    file1_key = 'a1'

    # 同时支持读取csv和xlsx
    file2_name_list = ['file12.xlsx', 'file13.xlsx']
    file2_key = 'a2'
    file2_select_column_list = ['b2', 'c2', 'd2']
    file2_collumn_date = 'd2'

    # 有一种特殊逻辑如果file1_key字段的字符串长度是18的时候，如果file2_key字段长度为36时，如果file2_key中前18个或者后18个字符串和key1相等就可以被选中
    special_key2_len_list = [6]  # 特殊两个key拼接在一起的key2字段长度, 例如34，36

    output_file_name = 'output2.xlsx'
    output_file_column_list = ['f1', 'g1', 'h1']

    # 使用函数
    merge_files(file1_name, file1_key, file2_name_list, file2_key, file2_select_column_list, file2_collumn_date,
                special_key2_len_list, output_file_name, output_file_column_list)
