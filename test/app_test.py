from readTablefromFinDB import readTablefromFinDBusingConfigMenu
import time

if __name__=="__main__":

    # configure for test1, a simple test for csmar data
    concat_df_cfg1={
        # 基础配置
        "runtime_code":"cyj", # 运行时代码，必选项，用于区分不同的运行时，用于输出文件名的开头，应该是字符串格式
        "data_source":"zip", # 数据来源，必选项，可选zip和folder，即从压缩文件中读入或从文件夹中读入，应该是字符串格式
        "target_folder":r"rawdata4test1", # 待处理数据所在的文件夹，必选项，如果是zip文件也需要放在一个文件夹中，应该是字符串格式的文件夹路径
        "skiprows":[1,2], # 读取表格时要跳过的行，从0开始计数。从CSMAR下载的数据一般需要跳过[1,2]，即中文表名列与单位列；从CNRDS下载的数据一般需要跳过[1]
        "output_filename":"test1.pkl",# 输出文件名，可选项，默认为None即不需要输出，为防止文本中逗号与逗号分隔符混淆不支持csv，支持xlsx，数据量较大时会以1000000行为分割输出多个excel文件，支持pickle(.pkl)格式，输出文件会出现在finalresults文件夹中
        "filter_conditions":{ # 筛选条件，可选项，默认为None即不需要筛选，仅用于简单的筛选操作，复杂的筛选操作请在结果输出后自行处理，不同筛选条件之间的关系是逻辑与，运行时筛选的好处是可以减少内存占用
            "start_date":"2012", # 数据开始日期（或年月），可选项，默认为None即不需要筛选，若定义则在该日期之前的数据将被删去，仅在索引为时间序列格式时有效，可以为字符串格式或时间格式
            "end_date":"2022", # 数据结束日期（或年月），可选项，默认为None即不需要筛选，，在该日期之后的数据将被删去，仅在索引为时间序列格式时有效，可以为字符串格式或时间格式
            "not_str_filter_conditions":[ # 非字符串筛选条件，可选项，默认为None即不需要筛选，若需要筛选请用如下格式定义，可以添加多个字典
                {
                    "field_name":"Zvalue", # 筛选所用的字段名称
                    "field_function":None, # 通过函数对字段值进行映射，支持len等函数，python内置函数可以定义为用函数或字符串形式，自定义函数只能以函数形式传入，为避免无法序列化进入多进程请不要定义高级函数（例如闭包）
                    "operator":"<", # 操作符，以字符串形式定义，支持in,not in,>,<,=,>=,<=,==,!=等操作符
                    "value":2 # 比较值
                },
            ],
            "str_filter_conditions":[ # 字符串筛选条件，可选项，默认为None即不需要筛选，若需要筛选请用如下格式定义，可以添加多个字典
                {
                    "field_name":"ShortName", # 筛选所用的字段名称
                    "operator":"endswith", # 操作符，以字符串形式定义，支持contains,startswith,endswith等操作符，支持正则表达式
                    "value":"投债" # 比较值
                },
            ],
        },
    }

    # configure for test2, a simple test for cnrds data
    concat_df_cfg2={
        # 基础配置
        "runtime_code":"cyj", # 运行时代码，必选项，用于区分不同的运行时，用于输出文件名的开头，应该是字符串格式
        "data_source":"folder", # 数据来源，必选项，可选zip和folder，即从压缩文件中读入或从文件夹中读入，应该是字符串格式
        "target_folder":r"rawdata4test2", # 待处理数据所在的文件夹，必选项，如果是zip文件也需要放在一个文件夹中，应该是字符串格式的文件夹路径
        "usecols":"Scode	Coname	Trddt	Dopnprc	Dhiprc	Dloprc	Dclsprc	Dclsprcp	Dret	Adret	Dtrdamt	Dts	Dos", # 表格中要读取的列的列名，可选项，默认为'all'即读取所有列，若指定则只读取指定列，指定的格式为list[str]或list[list[str],list[str],...]或以空格分隔的字段名（方便直接从excel中复制）或包含以空格分隔的字段名的字典
        "ts_index_column_name":"Trddt", # 作为索引的时间序列的列名，可选项，默认为"auto"即根据列名是否以date|dt|month|mnt|year|yr开头或结尾自动判断，可以用字符串格式指定，若指定则会自动转为pd.DateTimeIndex格式并升序排列，若设为None不指定则不会转换时间序列格式
        "skiprows":[1], # 读取表格时要跳过的行，可选项，若data_source为zip默认为[1,2]，若data_source为folder默认为[1]，从0开始计数。从CSMAR下载的数据一般需要跳过[1,2]，即中文表名列与单位列；从CNRDS下载的数据一般需要跳过[1]
        "convert_str_columns":"Scode", # 需要被强制转为字符串类型的列的列名，可选项，默认为"auto"即根据列名是否以id|cd|code|symbol开头或结尾自动判断，作用是防止股票代码等以一串数字标识但不应该参与代数运算的字符串被当作其他格式处理，可以通过list[str]或list[list[str],list[str],...]或以空格分隔的字段名或包含以空格分隔的字段名的字典格式手动指定列名，不需要转换可以设置为None
        "output_filename":"test2.pkl", # 输出文件名，可选项，默认为None即不需要输出，为防止文本中逗号与逗号分隔符混淆不支持csv，支持xlsx，数据量较大时会以1000000行为分割输出多个excel文件，支持pickle(.pkl)格式，输出文件会出现在finalresults文件夹中
    }
  
    print("程序运行开始\n")
    
    print("开始运行test1")
    t0=time.time()
    rtn_catdf=readTablefromFinDBusingConfigMenu(config_menu=concat_df_cfg1)
    print(rtn_catdf.head(10))
    print("test1运行完成，用时{:.4f}秒".format(time.time()-t0))
    
    print("开始运行test2")
    t0=time.time()
    rtn_catdf=readTablefromFinDBusingConfigMenu(config_menu=concat_df_cfg2)
    print(rtn_catdf.head(10))
    print("test2运行完成，用时{:.4f}秒".format(time.time()-t0))
    
    print("测试程序运行结束，没有检测到任何异常，你可以检查finalresults文件夹以查看产生的结果文件，请尝试修改配置文件并重新运行以观察变化")