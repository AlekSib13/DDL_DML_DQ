import pandas as pd,numpy as np, matplotlib as mpl
import os,re,datetime

class DatasetCreator:
    def __init__(self,path):
        self.path=path
        self.check_path=self.__check_path_existance()

    def __check_path_existance(self):
        if os.path.exists(self.path):
            def create_datasets():
                files_list=os.listdir(self.path)
                if files_list:
                    dict_={}
                    for file in files_list:
                        if re.findall('\.xlsx',file):
                            key=re.findall('(\w+)\.',file)[0]
                            dict_[key]=pd.read_excel(os.path.join(self.path,file))
                        elif re.findall('\.csv',file):
                            key = re.findall('(\w+)\.', file)[0]
                            dict_[key] = pd.read_csv(os.path.join(self.path,file))
                    if len(dict_)!=0:
                        return dict_

                    else:
                        print('No files with required extension')
                else:
                    print('Folder is empty')

            return create_datasets()

        else:
            result_feedback='Path does not exist'
            print(result_feedback)


#dataset_creator=DatasetCreator(input('Введите путь: '))
dataset_creator=DatasetCreator(r'W:\Сибур\ФЦТ\04 Направления\02 Работа с данными\DataExchange\Logistic_UCP\LogisticsMetrics\prod\auto_metrics\data')


datasets=dataset_creator.check_path


def dublicate_checker(datasets_list):
    datasets_list_pk={}
    for dataset_name,dataset in datasets_list.items():
            pk_dict={}
            for pk in dataset.values[:,0]:
                if pk not in pk_dict:
                    pk_dict[pk]=0
                pk_dict[pk]+=1
            for pair in list(pk_dict.items()):
                if pair[1]==1:
                    pk_dict.pop(pair[0])
            if len(pk_dict)!=0:
                datasets_list_pk[dataset_name] = pk_dict
    return datasets_list_pk



def dublicate_dataset_creator(list_of_datasets,save_result_to=False):
    row=[]
    for key,values_list in list_of_datasets.items():
        for pk,value in values_list.items():
            row.append(np.array((datetime.datetime.today().strftime('%Y-%m-%d'),key,pk,value)))

    dataset_array=np.reshape(row,(-1,4))
    dataset=pd.DataFrame(dataset_array,columns=['date','excel_source','pk_num','number_of_dublicates'])

    if not save_result_to:
        if os.path.exists(some_route):
            return dataset.to_csv(some_route,index=False,mode='a',header=None)
        else:
            return dataset.to_csv(some_route,
                                  index=False, mode='a')
    else:
        if os.path.exists(save_result_to):
            return dataset.to_csv(save_result_to,
                              index=False, mode='a',header=None)
        else:
            return dataset.to_csv(save_result_to,
                                  index=False, mode='a')



def func_manager(save_result_to=False):
    dublicates_dict=dublicate_checker(datasets)
    result=dublicate_dataset_creator(dublicates_dict,save_result_to)

    return result

func_manager(some_route)



