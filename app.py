from concat_data_frame import concatDF
import time
import os

if __name__=="__main__":

    # 合并表格配置文件
    concat_df_cfg={
        "runtime_code":"cyj", # 运行时代码，用于区分不同的运行时，用于输出文件名的开头
        "data_source":"zip", # 数据来源，可选zip和folder，即从压缩文件中读入或从文件夹中读入
        "target_folder":r"your_target_folder", # 待处理数据所在的文件夹，如果是zip文件也需要放在一个文件夹中
        "csv_delimiter":"\t", # csv分隔符，仅在读取csv文件时生效
        "usecols":"all", # 表格中要读取的列的列名，若指定则只读取指定列，若不指定则读取所有列，默认为all即读取所有列；指定的格式为list[str]或list[list[str],list[str],...]
        "ts_index_column_name":None, # 作为索引的时间序列的列名，若指定则会自动转为pd.DateTimeIndex格式并升序排列，若不指定则不会转换时间序列格式，默认为None即不指定，指定的格式为str
        "skiprows":[1,2], # 读取表格时要跳过的行，从0开始计数。从CSMAR下载的数据一般需要跳过[1,2]，即中文表名列与单位列；从CNRDS下载的数据一般需要跳过[1]，默认为None即不指定
        "convert_str_columns":"auto", # 需要被强制转为字符串类型的列的列名，以防止字符串被当作其他格式处理，可以选择auto根据列名是否以id,cd,code,symbol开头或结尾自动判断，或者以list[str]或list[list[str],list[str],...]格式手动指定列名，不需要转换则设为空列表或None
        "output_filename":"your_output_filename.pkl/xlsx", # 表格合并程序输出文件名，为防止文本中逗号与逗号分隔符混淆不支持csv，支持xlsx，数据量较大时会以1000000行为分割输出多个excel文件，支持pickle(.pkl)格式
        "zip_starts_with":"all", # 压缩文件名开头，当target_folder中有多种数据时可以据此选择性读入想要的，默认为"all"，当定义为列表或元组时会根据共同列横向合并多个表格，仅在data_source为zip时有效
        "filter_conditions":None, # 筛选条件，仅用于简单的筛选操作，复杂的筛选操作请在结果输出后自行处理，不需要可以设为空字典或None，不同筛选条件之间的关系是逻辑与，运行时筛选的好处是可以减少内存占用
        # "filter_conditions":{ # 筛选条件的定义格式
        #     "start_date":None, # 数据开始日期（或年月），在该日期之前的数据将被删去，None则为从原始输入的最早日期开始，仅在索引为日期格式时有效
        #     "end_date":None, # 数据结束日期（或年月），在该日期之后的数据将被删去，None则为从原始输入的最晚日期结束，仅在索引为日期格式时有效
        #     "not_str_filter_conditions":[ # 非字符串筛选条件，不需要可以设为空列表或None
        #         {
        #             "field_name":"ClassifyName", # 筛选所用的字段名称
        #             "field_function":None, # 通过函数对字段值进行映射，支持len等函数，python内置函数可以定义为用函数或字符串形式，自定义函数只能以函数形式传入，暂不支持当定义为列表或元组时多个函数按顺序调用
        #             "operator":"in", # 操作符，以字符串形式定义，支持in,not in,>,<,=,>=,<=,==,!=等操作符
        #             "value":["新闻","时事要闻","国内时事","国际时事","新闻(内地财经)","综合消息(宏观新闻)","财经新闻(内地财经)","海外财经"] # 比较值
        #         },
        #     ],
        #     "str_filter_conditions":[ # 字符串筛选条件，不需要可以设为空列表或None
        #         {
        #             "field_name":"NewsContent", # 筛选所用的字段名称
        #             "operator":"contains", # 操作符，以字符串形式定义，支持contains,startswith,endswith等操作符，支持正则表达式
        #             "value":"天气" # 比较值
        #         },
        #     ],
        #     "not_allow_nan_columns":["NewsContent"], # 不允许有缺失值的列，删去定义的列中有空缺值的观测，None为不删去任何观测，"all"为删去所有有空缺值的观测
        #     "str_filter_conditions":None, # 字符串筛选条件，不需要可以设为空列表或None
        # },
        # 以下两项respwanpoint文件夹配置，respawnpoint文件夹用于存储临时文件，以避免在读取大量数据时占用过多内存造成程序崩溃，若不清空respawnpoint文件夹可能导致之前的临时文件被重复读入
        "clear_respawnpoint_before_run":True, # 开始运行前是否清空respawnpoint文件夹
        "clear_respawnpoint_upon_conplete":True, # 完成后是否清空respawnpoint文件夹
    }
  
    print("程序运行开始\n")
    t0=time.time()
    if "respawnpoint" not in os.listdir():
        print(f"在工作目录{os.getcwd()}下未找到用于存储临时文件的respawnpoint文件夹，将自动创建")
        os.mkdir("respawnpoint")
    if "finalresults" not in os.listdir():
        print(f"在工作目录{os.getcwd()}下未找到用于存储最终结果的finalresults文件夹，将自动创建")
        os.mkdir("finalresults")
    concatDF(concat_df_cfg["runtime_code"],concat_df_cfg["data_source"],concat_df_cfg["target_folder"],concat_df_cfg["usecols"],concat_df_cfg["ts_index_column_name"],concat_df_cfg["filter_conditions"],concat_df_cfg["csv_delimiter"],concat_df_cfg["convert_str_columns"],concat_df_cfg["output_filename"],concat_df_cfg["skiprows"],concat_df_cfg["zip_starts_with"],concat_df_cfg["clear_respawnpoint_before_run"],concat_df_cfg["clear_respawnpoint_upon_conplete"])
    print(f"\n程序运行完成，用时{(time.time()-t0):.4f}秒") # It is very weird that this line will not conduct until waiting for a very long period (about 10min) and the memory usage holds at a very high level. It seems to be a problem triggered by lingered subprocesses, but when I use breakpoint debug to observe, the problem disappeared.