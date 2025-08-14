from zipfile import is_zipfile,ZipFile
import multiprocessing as mp
from functools import reduce
from io import BytesIO
import pickle
import os
import re

import pandas as pd
import numpy as np

import warnings
warnings.filterwarnings("ignore",category=UserWarning,module='openpyxl') # to supress UserWarning: Workbook contains no default style, apply openpyxl's default

# common tool functions are as follows
def forceConvertIntoDatetimeIndex(df,fileName="表格"):
    # iterate through the index of the dataframe and convert it into datetime index
    if type(df.index)==pd.DatetimeIndex:
        df=df.sort_index(ascending=True)
        return df
    while True:
        try:
            df.index=pd.DatetimeIndex(df.index)
            break
        except pd._libs.tslibs.parsing.DateParseError as e:
            tmp=re.match(r"Unknown datetime string format, unable to parse: (.+), at position (\d+)",str(e))
            print(f"在{fileName}中无法将\'{tmp.group(1)}\'解析为日期或时间格式，删去以\'{tmp.group(1)}\'为索引的{sum(df.index==tmp.group(1))}行数据")
            df=df[df.index!=tmp.group(1)]
            print(f"操作后{fileName}的剩余行数为{df.shape[0]}")
    df=df.sort_index(ascending=True)
    return df

def outputAsXlsx(df,output_filename,output_pathname,thereshold_rows=1000000,thereshold_GB=4):
    # output the dataframe as xlsx file with divsion within the thereshold_rows and thereshold_GB
    # 搜寻正确的分块数
    def findBestBulkNum(df,thereshold_GB,best_bulk_num=1):
        for idx in range(best_bulk_num):
            memory_usage_GB=df.iloc[int(len(df)*(idx/best_bulk_num)):int(len(df)*((idx+1)/best_bulk_num))].memory_usage(deep=True).sum()/(1024**3)
            if memory_usage_GB>thereshold_GB:
                new_bulk_num=max(int(df.memory_usage(deep=True).sum()/(1024**3))//thereshold_GB+1,best_bulk_num+1)
                return findBestBulkNum(df,thereshold_GB,best_bulk_num=new_bulk_num)
        else:
            return best_bulk_num
    # 按分块数输出
    def outputAccording2BestBulkNum(df_bulk,fileName,thereshold_GB):
        bulk_num=findBestBulkNum(df_bulk,thereshold_GB)
        if bulk_num==1:
            df_bulk.to_excel(fileName)
        else:
            print(f"文件{fileName}所需的存储空间超过阙值{thereshold_GB}GB，再分为{bulk_num}个文件输出")
            for iidx in range(bulk_num):
                fileName_=f"{''.join(fileName.split('.')[:-1])}_{iidx+1}.xlsx"
                print(f"正在写入{fileName_}")
                df_bulk.iloc[int(file_rows*(iidx/bulk_num)):int(file_rows*((iidx+1)/bulk_num))].to_excel(fileName_)
        return None
    # 先按照行数阙值分为file_num+1个文件输出，对每个输出文件检查存储空间大小并根据最优文件数输出
    file_num=int(df.shape[0]//thereshold_rows)
    print(f"共{df.shape[0]}行，文件名为{output_filename}，分为{file_num+1}个文件输出")
    if file_num==0:
        outputAccording2BestBulkNum(df,fileName=f"{output_pathname}{'' if output_pathname.endswith('/') else '/'}{''.join(output_filename.split('.')[:-1])}.xlsx",thereshold_GB=thereshold_GB)
    else:
        file_rows,last_rows=divmod(df.shape[0],file_num+1)
        last_rows=file_rows+last_rows
        print(f"前{file_num}个文件{file_rows}行，最后1个文件{last_rows}行")
        for idx in range(file_num):
            df_bulk=df.iloc[idx*file_rows:(idx+1)*file_rows]
            fileName=f"{output_pathname}{'' if output_pathname.endswith('/') else '/'}{''.join(output_filename.split('.')[:-1])}_{idx+1}.xlsx"
            print(f"正在写入{fileName}")
            outputAccording2BestBulkNum(df_bulk,fileName,thereshold_GB)
        if last_rows:
            df_bulk=df.iloc[file_num*file_rows:]
            fileName=f"{output_pathname}{'' if output_pathname.endswith('/') else '/'}{''.join(output_filename.split('.')[:-1])}_{file_num+1}.xlsx"
            print(f"正在写入{fileName}")
            outputAccording2BestBulkNum(df_bulk,fileName,thereshold_GB)
    return None

# common data processing functions are as follows
def filterDF(df,filter_conditions,dfName="表格"):
    # filter the dataframe according to the filter_conditions
    usecols=set(df.columns)
    # 根据时间序列索引截取数据
    if (filter_conditions["start_date"] or filter_conditions["end_date"]) and type(df.index)!=pd.DatetimeIndex:
        raise ValueError(f"指定了时间序列索引截取条件但{dfName}的索引不是时间序列，请核查")
    df=df[filter_conditions["start_date"]:filter_conditions["end_date"]]
    print(f"经时间序列索引筛选后{dfName}剩余{df.shape[0]}个样本，有{df.shape[1]}个变量")
    # 根据非字符串条件筛选表格
    if filter_conditions["not_str_filter_conditions"]:
        for condition in filter_conditions["not_str_filter_conditions"]:
            if condition["field_name"] in usecols:
                if field_function:=condition["field_function"]:
                    if type(field_function)==str:
                        df=df[eval(f"df[condition['field_name']].apply(globals()['__builtin__'].__getattribute__(field_function)) {condition['operator']} {condition['value']}")]
                    else:
                        df=df[eval(f"df[condition['field_name']].apply(field_function) {condition['operator']} condition['value']")]
                else:
                    df=df.query(f"{condition['field_name']} {condition['operator']} {condition['value']}")
    print(f"经非字符串条件筛选后{dfName}剩余{df.shape[0]}个样本，有{df.shape[1]}个变量")
    # 根据字符串条件筛选表格
    if filter_conditions["str_filter_conditions"]:
        for condition in filter_conditions["str_filter_conditions"]:
            if condition["field_name"] in usecols:
                df=df[df[condition['field_name']].str.__getattribute__(condition['operator'])(condition['value'],na=False)]
    print(f"经字符串筛选后{dfName}剩余{df.shape[0]}个样本，有{df.shape[1]}个变量")
    # 根据空缺值筛选表格
    if filter_conditions["not_allow_nan_columns"]:
        df=df.dropna(subset=set.intersection((set((filter_conditions["not_allow_nan_columns"],)) if type(filter_conditions["not_allow_nan_columns"])==str else set(filter_conditions["not_allow_nan_columns"])),usecols))
    print(f"剔除带空缺观测的行后{dfName}剩余{df.shape[0]}个样本，有{df.shape[1]}个变量")
    return df

def saveConcatedDataAsFinalResult(runtime_code,concatedDF,output_filename,clear_respawnpoint_upon_conplete):
    # the end process of the concatDF, including writing the final result to the disk and clear the respawnpoint folder
    if not clear_respawnpoint_upon_conplete or not output_filename:
        pickle.dump(concatedDF,open(f"respawnpoint/{runtime_code}_news_info.pkl","wb"))
    if output_filename:
        print("开始将最终结果写入硬盘")
        if output_filename.endswith(".pkl"):
            pickle.dump(concatedDF,open(f"finalresults/{output_filename}","wb"))
        elif output_filename.endswith(".xlsx"):
            outputAsXlsx(concatedDF,output_filename,"finalresults")
        elif output_filename.endswith(".csv"):
            concatedDF.to_csv(f"finalresults/{output_filename}")
        else:
            raise ValueError(f"不支持的文件格式{output_filename}，请核查")
    if clear_respawnpoint_upon_conplete:
        if not output_filename and input("由于未指定output_filename，finalresults文件夹中不会产生任何结果文件，又clear_respawnpoint_upon_conplete参数为True，将清空respawnpoint文件夹中的所有临时文件，因此您的本次运行不会产生任何可观测的结果，输入y继续，输入其他任意字符取消清空respawnpoint文件夹：").lower()!="y":
            print("用户取消清空respawnpoint文件夹")
            return False # return False to indicate that nothing emerge in either finalresults or respawnpoint
        print("开始清空respawnpoint文件夹")
        for file in os.listdir("respawnpoint/"):
            os.remove("respawnpoint/" + file)
    return None

# csmar functions are as follows
def getUseColsFromZipFile(target_folder,zip_prefix,skiprows):
    # get the column names of the first xlsx or csv file in the first zip file with specified prefix
    tgtZip=(file for file in os.listdir(target_folder) if is_zipfile(f"{target_folder}/{file}") and file.startswith(zip_prefix)).__next__()
    with ZipFile(f"{target_folder}/{tgtZip}") as myzip:
        for fileInfo in myzip.filelist:
            filename=fileInfo.filename
            file_ext=filename.split(".")[-1] 
            if file_ext in ["xlsx","csv"]:
                if file_ext=="xlsx":
                    with myzip.open(filename,"r") as myfile:
                        usecols=tuple(pd.read_excel(BytesIO(myfile.read()),skiprows=skiprows,nrows=0).columns)
                elif file_ext=="csv":
                    with myzip.open(filename,"r") as myfile:
                        usecols=tuple(pd.read_csv(BytesIO(myfile.read()),skiprows=skiprows,nrows=0).columns)
                print(f"对于以'{zip_prefix}'为前缀的压缩文件，通过读取{tgtZip}/{filename}自动推断usecols为{usecols}")
                break
        else:
            raise FileNotFoundError(f"在目标压缩文件{target_folder}/{tgtZip}中未找到任何以xlsx或csv为后缀的文件")
    return usecols,zip_prefix

def checkColumnNamesValidity(usecols,ts_index_column_name,skiprows,target_folder,zip_starts_with=None):
    # check whether there are overlapped columns in different zip files
    if usecols[0][0]=="auto" or usecols[0][0]=="all":
        if zip_starts_with: # csmar mode
            usecols_inferred=tuple((getUseColsFromZipFile(target_folder,zip_prefix,skiprows=skiprows) for zip_prefix in zip_starts_with))
            usecols=[usecol[0] for usecol in usecols_inferred]
            common_columns_4_index=set.intersection(*(set(usecol[0]) for usecol in usecols_inferred))
        else: # cnrds mode
            usecols_inferred=tuple(pd.read_excel(target_folder[0],skiprows=skiprows,nrows=0).columns)
            common_columns_4_index=set(usecols_inferred)
    else:
        if zip_starts_with and len(usecols)!=len(zip_starts_with):
            raise ValueError(f"指定的{usecols=}与{zip_starts_with=}的长度不匹配，请核查")
        common_columns_4_index=set.intersection(*(set(usecol) for usecol in usecols))
    if ts_index_column_name=="auto":
        potential_ts_idx_col=tuple(col for col in common_columns_4_index if re.search(r"(^(date|dt|month|mnt|year|yr|Accper))|((date|dt|month|mnt|year|yr|Accper)$)",col,flags=re.I))
        if len(potential_ts_idx_col)==0:
            print(f"在共同列{common_columns_4_index}中没有找到看似可以作为时间序列索引的列名")
            ts_index_column_name=None
        elif len(potential_ts_idx_col)==1:
            if input(f"在共同列{common_columns_4_index}中发现了唯一看似可以作为时间序列索引的列名{potential_ts_idx_col[0]}，是否将其作为时间序列索引，输入y确认，输入其他任意字符取消：").lower()=="y":
                ts_index_column_name=potential_ts_idx_col[0]
            else:
                ts_index_column_name=None
        else:
            print(f"在共同列{common_columns_4_index}中发现了多个看似可以作为时间序列索引的列名{dict(enumerate(potential_ts_idx_col))}")
            res=input("您要将哪一列作为时间序列索引，请输入序号，若不需要时间序列索引可以输入其他任意字符取消：")
            if res.isdigit() and int(res) in range(len(potential_ts_idx_col)):
                ts_index_column_name=potential_ts_idx_col[int(res)]
                print(f"您选择了{ts_index_column_name}作为时间序列索引")
            else:
                print(f"您的输入{res}不在可选的序号中，视为不需要时间序列索引")
                ts_index_column_name=None
    if ts_index_column_name and ts_index_column_name not in common_columns_4_index:
        raise ValueError(f"指定的{ts_index_column_name=}不在{common_columns_4_index=}中，请核查")
    if not ts_index_column_name:
        print(f"根据共同列名建立联合索引{common_columns_4_index}")
    overlapped_columns_besides_common_columns_4_index=list()
    for i in range(len(usecols)-1):
        for j in range(i+1,len(usecols)):
            if (overlapped_cols:=set(usecols[i]).intersection(set(usecols[j]))-common_columns_4_index)!=set():
                overlapped_columns_besides_common_columns_4_index.append((zip_starts_with[i],zip_starts_with[j],overlapped_cols))
    common_columns_4_index=list(common_columns_4_index)
    return common_columns_4_index,overlapped_columns_besides_common_columns_4_index,usecols,ts_index_column_name

def getDataFiles4OneZipPrefix(target_folder,zip_prefix):
    # find all zip files in the target folder with specified prefix
    news_info_zips=[file for file in os.listdir(target_folder) if is_zipfile(f"{target_folder}/{file}") and file.startswith(zip_prefix)]
    # iterate through all zip files in news_info_zips and find the data files in each zip file
    data_file_path=list()
    for zip_filename in news_info_zips:
        with ZipFile(f"{target_folder}/{zip_filename}") as myzip:
            for fileInfo in myzip.filelist:
                data_filename=fileInfo.filename
                if data_filename.split(".")[-1]  in ["xlsx","csv"]:
                    data_file_path.append((zip_prefix,zip_filename,data_filename))
    return data_file_path

def readDataFileFromZipFile(chunk):
    runtime_code,zip_prefix,target_folder,zip_filename,data_filename,usecols,ts_index_column_name,skiprows,csv_delimiter,convert_str_columns,filter_conditions=chunk
    # read the data file from the zip file
    try:
        file_ext=data_filename.split(".")[-1]
        with ZipFile(f"{target_folder}/{zip_filename}") as myzip:
            with myzip.open(data_filename,"r") as myfile:
                if file_ext=="xlsx":
                    df=pd.read_excel(BytesIO(myfile.read()),index_col=ts_index_column_name,skiprows=skiprows,usecols=usecols,converters={key:str for key in convert_str_columns if convert_str_columns})
                elif file_ext=="csv":
                    df=pd.read_csv(BytesIO(myfile.read()),index_col=ts_index_column_name,skiprows=skiprows,usecols=usecols,delimiter=csv_delimiter,converters={key:str for key in convert_str_columns if convert_str_columns})
                else:
                    raise ValueError(f"未知的文件格式{file_ext}在{zip_filename}/{data_filename}中")
            print(f"读取{zip_filename}/{data_filename}完成，有{df.shape[0]}个样本，{df.shape[1]}个变量")
        if ts_index_column_name:
            df=forceConvertIntoDatetimeIndex(df,f"{zip_filename}/{data_filename}")
        if filter_conditions:
            df=filterDF(df,filter_conditions,f"{zip_filename}/{data_filename}")
        if ts_index_column_name:
            df=df.reset_index() # here we reset index so that in concated_df=reduce(lambda df1,df2:df1.join(df2,how="outer"),(concated_df[1].set_index(common_columns_4_index) for concated_df in concated_dfs)) in concatDF function can work, else the time series index will be lost
        while True:
            store_path=f"respawnpoint/{runtime_code}_{zip_prefix}_{np.random.randint(10000,100000)}.pkl"
            if not os.path.exists(store_path):
                break
        pickle.dump(df,open(store_path,"wb")) # here we store the data in a pickle file instead of directly return it to avoid a large amount of data being stored in the memory
        return zip_prefix,store_path
    except Exception as e:
        raise RuntimeError(f"{zip_filename}/{data_filename}未能正常解析请核查，原因是{e}")

def concatDataFilesNachZipPrefix(runtime_code,zip_starts_with,target_folder,read_table_params,ts_index_column_name,skiprows,csv_delimiter,filter_conditions):
    # concat the data files according to the same zip prefix
    data_file_paths=[getDataFiles4OneZipPrefix(target_folder,zip_prefix) for zip_prefix in zip_starts_with]
    if not data_file_paths:
        raise FileNotFoundError(f"在目标文件夹{target_folder}中没有找到任何以{zip_starts_with}开头的压缩文件")
    chunks=tuple(tuple((runtime_code,zip_prefix,target_folder,zip_filename,data_filename,read_table_params[zip_prefix][0],ts_index_column_name,skiprows,csv_delimiter,read_table_params[zip_prefix][1],filter_conditions) for zip_prefix,zip_filename,data_filename in data_file_path) for data_file_path in data_file_paths)
    chunks=tuple(chunk for chunks_with_same_zip_prefix in chunks for chunk in chunks_with_same_zip_prefix)
    with mp.Pool() as pool:
        results=pool.map(readDataFileFromZipFile,chunks)
    unique_zip_prefixes=set(result[0] for result in results)
    try:
        concatedDFs=tuple(tuple((zip_prefix,pd.concat((pickle.load(open(result[1],"rb")) for result in results if result[0]==zip_prefix),axis=0))) for zip_prefix in unique_zip_prefixes)
    except Exception as e:
        print(e)
        raise RuntimeError("在这一步出错通常是由于在将pickle文件写入硬盘时出现了错误，这个错误是偶然发生的且并不常见，重新运行程序通常可以解决此问题")
    return concatedDFs

def concatCsmarMain(runtime_code,target_folder,usecols,ts_index_column_name,filter_conditions,csv_delimiter,convert_str_columns,zip_starts_with,skiprows):
    # determine the read table parameters and transfer the parameters to the function concatDataFilesNachZipPrefix
    # static check
    if usecols[0][0]=="auto" or usecols[0][0]=="all":
        usecols=tuple(getUseColsFromZipFile(target_folder,zip_prefix)[0] for zip_prefix in zip_starts_with)
    if ts_index_column_name and ts_index_column_name not in (common_cols:=set.intersection(*(set(col) for col in usecols))):
        raise ValueError(f"指定的{ts_index_column_name=}不在{common_cols=}中，请核查")
    if filter_conditions["not_allow_nan_columns"]=="all":
        filter_conditions["not_allow_nan_columns"]=set(np.array(usecols).flatten().tolist())
    if convert_str_columns[0][0]=="auto":
        convert_str_columns=tuple(tuple(col for col in usecol if re.search(r"(^(symbol|code|id|cd))|((symbol|code|id|cd)$)",col,flags=re.I)) for usecol in usecols)
        print(f"自动推断convert_str_columns为{convert_str_columns}")
    if len(convert_str_columns)!=len(zip_starts_with):
        raise ValueError(f"指定的{convert_str_columns=}与{zip_starts_with=}的长度不匹配，请核查")
    # build a dictionary to store the specific parameters (i.e. usecols, convert_str_columns) for each zip file with zip_prefix as the key
    read_table_params=dict(zip(zip_starts_with,zip(usecols,convert_str_columns)))
    return concatDataFilesNachZipPrefix(runtime_code,zip_starts_with,target_folder,read_table_params,ts_index_column_name,skiprows,csv_delimiter,filter_conditions)

# cnrds functions are as follows
def concatOneCnrdsFile(chunk):
    # read and filter one data file directly as a xlsx or csv file (i.e. not read from a zip file)
    myfile,usecols,ts_index_column_name,filter_conditions,skiprows,csv_delimiter,convert_str_columns=chunk
    try:
        if myfile.split(".")[-1]=="xlsx":
            df=pd.read_excel(myfile,index_col=ts_index_column_name,usecols=usecols,skiprows=skiprows,converters={key:str for key in convert_str_columns if convert_str_columns})
        elif myfile.split(".")[-1]=="csv":
            df=pd.read_csv(myfile,index_col=ts_index_column_name,usecols=usecols,skiprows=skiprows,delimiter=csv_delimiter,converters={key:str for key in convert_str_columns if convert_str_columns})
        print(f"{myfile}共有{df.shape[0]}个样本，有{df.shape[1]}个变量")
        if ts_index_column_name:
            df=forceConvertIntoDatetimeIndex(df,myfile)
        if filter_conditions:
            df=filterDF(df,filter_conditions,myfile)
        return df
    except Exception as e:
        print(e)
        raise RuntimeError(f"{myfile}未能正常解析请核查")

def concatCnrdsMain(runtime_code,target_folder,usecols,ts_index_column_name,filter_conditions,csv_delimiter,convert_str_columns,output_filename,skiprows,clear_respawnpoint_upon_conplete):
    # walk through the target folder and find all xlsx or csv files to read and concat them
    news_info_folders=[]
    for filepath,_,filenames in os.walk(target_folder):
        for filename in filenames:
            if filename.split(".")[-1] in ["xlsx","csv"]:
                news_info_folders.append(os.path.join(filepath,filename))
    common_columns_4_index,_,_,ts_index_column_name=checkColumnNamesValidity(usecols,ts_index_column_name,skiprows,news_info_folders)
    if convert_str_columns[0][0]=="auto":
        convert_str_columns=tuple(col for col in common_columns_4_index if re.search(r"(^(symbol|code|id|cd))|((symbol|code|id|cd)$)",col,flags=re.I))
        print(f"自动推断convert_str_columns为{convert_str_columns}")
    chunks=tuple(tuple((file,common_columns_4_index,ts_index_column_name,filter_conditions,skiprows,csv_delimiter,convert_str_columns)) for file in news_info_folders)
    with mp.Pool() as pool:
        results=pool.map(concatOneCnrdsFile,chunks)
    concated_df=pd.concat(results,axis=0)
    concated_df.sort_index(inplace=True)
    saveConcatedDataAsFinalResult(runtime_code,concated_df,output_filename,clear_respawnpoint_upon_conplete)
    return concated_df

# main function
def readTablefromFinDB(runtime_code,data_source,target_folder,csv_delimiter,usecols,ts_index_column_name,skiprows,convert_str_columns,output_filename,zip_starts_with,filter_conditions,clear_respawnpoint_before_run,clear_respawnpoint_upon_conplete):
    print("合并表格模块开始运行")
    # create folder if not exists
    if "respawnpoint" not in os.listdir():
        print(f"在工作目录{os.getcwd()}下未找到用于存储临时文件的respawnpoint文件夹，将自动创建")
        os.mkdir("respawnpoint")
    if "finalresults" not in os.listdir():
        print(f"在工作目录{os.getcwd()}下未找到用于存储最终结果的finalresults文件夹，将自动创建")
        os.mkdir("finalresults")
    # clear the respawnpoint folder before running if needed
    if clear_respawnpoint_before_run:
        for file in os.listdir("respawnpoint/"):
            os.remove("respawnpoint/"+file)
    # check and modify the input parameters
    if type(usecols)==str:
        usecols=usecols.strip().split()
        usecols=(usecols,)
    if type(convert_str_columns)==str:
        convert_str_columns=convert_str_columns.strip().split()
        convert_str_columns=(convert_str_columns,)
    if (usecols[0][0]=="all" and usecols[0][0]=="auto") and (convert_str_columns[0][0]!="auto"):
        raise ValueError("当usecols为'all'或'auto'时convert_str_columns也应为'auto'，否则无法正确推断读取文件的顺序")
    if filter_conditions:
        if not filter_conditions.get("not_allow_nan_columns",None):
            filter_conditions["not_allow_nan_columns"]=set()
        filter_conditions["start_date"]=filter_conditions.get("start_date",None)
        filter_conditions["end_date"]=filter_conditions.get("end_date",None)
        filter_conditions["not_str_filter_conditions"]=filter_conditions.get("not_str_filter_conditions",None)
        filter_conditions["str_filter_conditions"]=filter_conditions.get("str_filter_conditions",None)
        if type(filter_conditions["start_date"])==int: # 2010=>"2010"
            filter_conditions["start_date"]=str(filter_conditions["start_date"])
        if type(filter_conditions["end_date"])==int:
            filter_conditions["end_date"]=str(filter_conditions["end_date"])
    if not filter_conditions:
        filter_conditions={
            "start_date":None, # 数据开始日期（或年月），在该日期之前的数据将被删去，None则为从原始输入的最早日期开始
            "end_date":None, # 数据结束日期（或年月），在该日期之后的数据将被删去，None则为从原始输入的最晚日期结束
            "not_str_filter_conditions":None, # 非字符串筛选条件，不需要可以设为空列表或None
            "str_filter_conditions":None, # 字符串筛选条件，不需要可以设为空列表或None
            "not_allow_nan_columns":None, # 允许有缺失值的列，若该列有缺失值不会删去观测
        }
    # the csmar brench of the function
    if data_source.lower()=="zip" or data_source.lower()=="csmar":
        # check and modify the input parameters if zip_starts_with is not specified explicitly
        if (type(zip_starts_with)==str and (zip_starts_with=="all" or zip_starts_with=="auto")) or (type(zip_starts_with) in (list,tuple) and len(zip_starts_with)<1):
            if usecols[0][0]!="all" and usecols[0][0]!="auto":
                raise ValueError("当zip_starts_with为'all'或'auto'时usecols也应为'all'或'auto'，否则无法正确推断读取文件的顺序")
            zip_starts_with=tuple(set(re.match(r"(.+?)(\d+).*\.zip",filename).group(1) for filename in os.listdir(target_folder)))
            print(f"在目标文件夹{target_folder}中找到了{len(zip_starts_with)}个不同的文件前缀，分别为{zip_starts_with}")
            if not zip_starts_with:
                raise FileNotFoundError(f"在目标文件夹{target_folder}中找到了以下文件{os.listdir(target_folder)}，其中没有识别到任何符合csmar命名格式的zip文件，若您已经重命名了压缩文件请尝试手动指定需要被读取的压缩文件名，若您已经将文件解压缩请尝试将data_source指定为folder，若前两种情况未命中请检查您target_folder路径是否正确")
        if type(zip_starts_with)==str:
            zip_starts_with=(zip_starts_with,)
        # the main process of the concatDF function for csmar data
        if type(zip_starts_with) in (list,tuple):
            common_columns_4_index,overlapped_columns_besides_common_columns_4_index,usecols,ts_index_column_name=checkColumnNamesValidity(usecols,ts_index_column_name,skiprows,target_folder,zip_starts_with)
            concated_dfs=concatCsmarMain(runtime_code,target_folder,usecols,ts_index_column_name,filter_conditions,csv_delimiter,convert_str_columns,zip_starts_with,skiprows)
            print("表格读取完成，开始执行横向合并操作")
            for overlapped_column in overlapped_columns_besides_common_columns_4_index:
                for referenceResult in concated_dfs:
                    if referenceResult[0] in overlapped_column[0] or referenceResult[0] in overlapped_column[1]:
                        for col in overlapped_column[2]:
                            print(f"列{col}在{overlapped_column[0]}中的与{overlapped_column[1]}中重名，将{referenceResult[0]}中的列名重命名为'{col}_von_{referenceResult[0]}'")
                            referenceResult[1].rename(columns={col:f"{col}_von_{referenceResult[0]}"},inplace=True)
            concated_df=reduce(lambda df1,df2:df1.join(df2,how="outer"),(concated_df[1].set_index(common_columns_4_index) for concated_df in concated_dfs))
            if ts_index_column_name:
                concated_df=concated_df.reset_index().set_index(ts_index_column_name)
                concated_df=forceConvertIntoDatetimeIndex(concated_df,"concated_df")
            concated_df=filterDF(concated_df,filter_conditions,"concated_df") # Since we use outer join to concat the dataframes, the non-nan trait is not guaranteed, so we need filter again. Some may think a twice filter is redundant and recommend to drop the first filter when reading the data, but I think it is better to keep the first filter to avoid the memory overflow
            saveConcatedDataAsFinalResult(runtime_code,concated_df,output_filename,clear_respawnpoint_upon_conplete)
        else:
            raise ValueError(f"无效的输入{zip_starts_with=}，只能够传入str或list或tuple")
    # the cnrds brench of the function
    elif data_source.lower()=="folder" or data_source.lower()=="cnrds":
        concated_df=concatCnrdsMain(runtime_code,target_folder,usecols,ts_index_column_name,filter_conditions,csv_delimiter,convert_str_columns,output_filename,skiprows,clear_respawnpoint_upon_conplete)
    else:
        raise ValueError(f"无效的输入{data_source=}，该参数只接受zip, folder, csmar, cnrds")
    print("合并表格模块运行完成")
    return concated_df

def readTablefromFinDBusingConfigMenu(config_menu):
    # a shortcut to run the code using a config menu to avoid input to many parameters
    runtime_code=config_menu["runtime_code"]
    data_source=config_menu["data_source"]
    target_folder=config_menu["target_folder"]
    csv_delimiter=config_menu.get("csv_delimiter",",")
    usecols=config_menu.get("usecols","all")
    ts_index_column_name=config_menu.get("ts_index_column_name","auto")
    skiprows=config_menu.get("skiprows","my_null_indicator")
    if skiprows=="my_null_indicator":
        if data_source.lower() in ["zip","csmar"]:
            skiprows=[1,2]
        elif data_source.lower() in ["folder","cnrds"]:
            skiprows=[1]
        else:
            raise ValueError(f"用户没有指定参数{skiprows}，且'data_source'参数设置为{data_source}也不符合要求，无法进行合理推断")
    convert_str_columns=config_menu.get("convert_str_columns","auto")
    output_filename=config_menu.get("output_filename",None)
    zip_starts_with=config_menu.get("zip_starts_with","all")
    filter_conditions=config_menu.get("filter_conditions",None)
    clear_respawnpoint_before_run=config_menu.get("clear_respawnpoint_before_run",True)
    clear_respawnpoint_upon_conplete=config_menu.get("clear_respawnpoint_upon_conplete",True)
    return readTablefromFinDB(runtime_code,data_source,target_folder,csv_delimiter,usecols,ts_index_column_name,skiprows,convert_str_columns,output_filename,zip_starts_with,filter_conditions,clear_respawnpoint_before_run,clear_respawnpoint_upon_conplete)