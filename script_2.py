import pandas as pd


def merge_files(file1, a1, file2_name_list, a2, cols_to_copy, output_file, new_col_names):
    # 读取两个文件
    df1 = pd.read_excel(file1)
    df_list = []  # 用于存储每个文件的DataFrame

    for file_name in file2_name_list:
        df_temp = pd.read_excel(file_name)
        df_list.append(df_temp)

    # 使用pd.concat合并所有的DataFrame
    df2 = pd.concat(df_list)
    print(df2)

    # 检查new_col_names中的列名是否已经存在于df1中
    for col in new_col_names:
        if col in df1.columns:
            raise ValueError(f"Column name '{col}' already exists in {file1}")

    # 在df2中查找a2列数值等于a1列数值的行，并只保留每个重复组的第一行
    df2_filtered = df2.drop_duplicates(subset=a2, keep='first')

    # 使用merge函数合并两个dataframes
    df_merged = pd.merge(df1, df2_filtered, how='left', left_on=a1, right_on=a2)

    # 将指定列的内容复制到df1所在行数据的后面
    for col, new_col in zip(cols_to_copy, new_col_names):
        df1[new_col] = df_merged[col]

    # 输出到新的文件中
    df1.to_excel(output_file, index=False)


file1_name = 'file11.xlsx'
file1_key = 'a1'

file2_name_list = ['file12.xlsx', 'file13.xlsx']
file2_key = 'a2'
file2_select_column_list = ['b2', 'c2']

output_file_name = 'output2.xlsx'
output_file_column_list = ['f1', 'g1']

# 使用函数
merge_files(file1_name, file1_key, file2_name_list, file2_key, file2_select_column_list, output_file_name,
            output_file_column_list)
