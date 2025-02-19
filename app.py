from readTablefromFinDB import readTablefromFinDBusingConfigMenu
import time
import os

if __name__=="__main__":

    # 合并表格配置文件
    concat_df_cfg={
        "runtime_code":"cyj", # 运行时代码，必选项，用于区分不同的运行时，用于输出文件名的开头，应该是字符串格式
        "data_source":"zip", # 数据来源，必选项，可选zip和folder，即从压缩文件中读入或从文件夹中读入，应该是字符串格式
        "target_folder":r"your_target_folder_path", # 待处理数据所在的文件夹，必选项，如果是zip文件也需要放在一个文件夹中，应该是字符串格式的文件夹路径
        "csv_delimiter":",", # csv分隔符，可选项，默认为英语逗号，仅在读取csv文件时生效
        "usecols":"all", # 表格中要读取的列的列名，可选项，默认为'all'即读取所有列，若指定则只读取指定列，指定的格式为list[str]或list[list[str],list[str],...]或以空格分隔的字段名（方便直接从excel中复制）或包含以空格分隔的字段名的字典
        "ts_index_column_name":"auto", # 作为索引的时间序列的列名，可选项，默认为"auto"即根据列名是否以date|dt|month|mnt|year|yr开头或结尾自动判断，可以用字符串格式指定，若指定则会自动转为pd.DateTimeIndex格式并升序排列，若设为None不指定则不会转换时间序列格式
        "skiprows":[1,2], # 读取表格时要跳过的行，可选项，默认为[1,2]，从0开始计数。从CSMAR下载的数据一般需要跳过[1,2]，即中文表名列与单位列；从CNRDS下载的数据一般需要跳过[1]
        "convert_str_columns":"auto", # 需要被强制转为字符串类型的列的列名，可选项，默认为"auto"即根据列名是否以id|cd|code|symbol开头或结尾自动判断，作用是防止股票代码等以一串数字标识但不应该参与代数运算的字符串被当作其他格式处理，可以通过list[str]或list[list[str],list[str],...]或以空格分隔的字段名或包含以空格分隔的字段名的字典格式手动指定列名，不需要转换可以设置为None
        "output_filename":"your_output_filename.pkl/xlsx", # 输出文件名，可选项，默认为None即不需要输出，为防止文本中逗号与逗号分隔符混淆不支持csv，支持xlsx，数据量较大时会以1000000行为分割输出多个excel文件，支持pickle(.pkl)格式，输出文件会出现在finalresults文件夹中
        "zip_starts_with":"all", # 压缩文件名开头，可选项，默认为"all"即读入target_folder中所有的压缩文件，用于当target_folder中有多种数据时可以据此选择性读入想要的，当定义为列表或元组时会根据共同列横向合并多个表格，当且仅当data_source为zip或csmar时有效
        "filter_conditions":{ # 筛选条件，可选项，默认为None即不需要筛选，仅用于简单的筛选操作，复杂的筛选操作请在结果输出后自行处理，不同筛选条件之间的关系是逻辑与，运行时筛选的好处是可以减少内存占用
            "start_date":None, # 数据开始日期（或年月），可选项，默认为None即不需要筛选，若定义则在该日期之前的数据将被删去，仅在索引为时间序列格式时有效，可以为字符串格式或时间格式
            "end_date":None, # 数据结束日期（或年月），可选项，默认为None即不需要筛选，，在该日期之后的数据将被删去，仅在索引为时间序列格式时有效，可以为字符串格式或时间格式
            "not_str_filter_conditions":[ # 非字符串筛选条件，可选项，默认为None即不需要筛选，若需要筛选请用如下格式定义，可以添加多个字典
                {
                    "field_name":"ClassifyName", # 筛选所用的字段名称
                    "field_function":None, # 通过函数对字段值进行映射，支持len等函数，python内置函数可以定义为用函数或字符串形式，自定义函数只能以函数形式传入，为避免无法序列化进入多进程请不要定义高级函数（例如闭包）
                    "operator":"in", # 操作符，以字符串形式定义，支持in,not in,>,<,=,>=,<=,==,!=等操作符
                    "value":["新闻","时事要闻","国内时事","国际时事","新闻(内地财经)","综合消息(宏观新闻)","财经新闻(内地财经)","海外财经"] # 比较值
                },
            ],
            "str_filter_conditions":[ # 字符串筛选条件，可选项，默认为None即不需要筛选，若需要筛选请用如下格式定义，可以添加多个字典
                {
                    "field_name":"NewsContent", # 筛选所用的字段名称
                    "operator":"contains", # 操作符，以字符串形式定义，支持contains,startswith,endswith等操作符，支持正则表达式
                    "value":"天气" # 比较值
                },
            ],
            "not_allow_nan_columns":["NewsContent"], # 不允许有缺失值的列，可选项，默认为None即所有的列都允许有空缺值，用于删去定义的列中有空缺值的观测，可以用一个列表指定，也可以指定为"all"表示删去所有有空缺值的观测
        },
        # 以下两项respwanpoint文件夹配置，respawnpoint文件夹用于存储临时文件，以避免在读取大量数据时占用过多内存造成程序崩溃，若不清空respawnpoint文件夹可能导致之前的临时文件被重复读入
        "clear_respawnpoint_before_run":True, # 开始运行前是否清空respawnpoint文件夹，可选项，默认为True，用于在运行前保持respwanpoint文件夹的清洁
        "clear_respawnpoint_upon_conplete":True, # 完成后是否清空respawnpoint文件夹，可选项，默认为True，若在下一次运行时还需要使用本次运行的部分结果，请设置为False
    }
  
    print("程序运行开始\n")
    t0=time.time()
    concated_df=readTablefromFinDBusingConfigMenu(config_menu=concat_df_cfg)
    print(concated_df.head(10))
    print(f"\n程序运行完成，用时{(time.time()-t0):.4f}秒") # It is very weird that when handling file of large size this line will not conduct until waiting for a very long period (about 10min) and the memory usage holds at a very high level. It seems to be a problem triggered by lingered subprocesses, but when I use breakpoint debug to observe, the problem disappeared.