import pandas as pd
import os
import logging
import statistics as st
import logging


def Building_dataframe(data_type, first_file):
    """ Import Evidence Json files and building a dataframes of all files"""

    logging.info('Reading first Json File')
    dataframe = pd.read_json('{}/{}'.format(data_type, first_file), lines=True)
    logging.info('Filtering columns')
    if data_type == 'evidence':
        dataframe = dataframe[['diseaseId', 'targetId', 'score']]

    dir_path = r'{}'.format(data_type)

    logging.info('start iterating on {} folder'.format(data_type))
    for path in os.listdir(dir_path):

        if os.path.isfile(os.path.join(dir_path, path)):

            if 'part-00000' not in path:
                df = pd.read_json('{}/{}'.format(data_type, path), lines=True)

                if data_type == 'evidence':
                    df = df[['diseaseId', 'targetId', 'score']]

                dataframe = dataframe.append(df)
                
    return dataframe


def median_top3(df_evidence):
    """ Calculating the median and Top3 scores """

    logging.info('Combining DiseaseId and TargetId')
    df_evidence['data'] = df_evidence['diseaseId'] + str('/') + df_evidence['targetId']

    logging.info('Filtering columns')
    df_evidence = df_evidence[['data', 'score']]

    logging.info('Grouping by the Column Data')
    df_evidence = df_evidence.groupby('data') \
        .agg(lambda x: sorted(list(x), reverse=True)) \
        .reset_index()
    
    logging.info('Calculating Median and Top3')
    df_evidence['median'] = df_evidence.apply(lambda x : st.median(x.score), axis=1)
    df_evidence['top3'] = df_evidence.apply(lambda x : list(set(x.score))[:3], axis=1)

    logging.info('Splitting the column data')
    df_evidence[['diseaseId', 'targetId']] = df_evidence['data'].str.split('/', 1, expand=True)

    logging.info('Ordering and Filtering columns')
    df_evidence = df_evidence.iloc[:,[4,5,2,3]]

    return df_evidence


def merge_data(evidence, diseases, targets):
    """ Merging 3 dataframes , diseases dataframe with column diseaseId , target dataframe with targetId """

    data = evidence.merge(diseases,left_on='diseaseId', right_on='id').merge(targets,left_on='targetId', right_on='id')
    return data


if "__main__" == __name__ :
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

    logging.info('Start building Evidence dataframe')
    evidence = Building_dataframe('evidence', 'part-00000-4134a310-5042-4942-82ed-565f3d91eddd.c000.json')

    logging.info('Start Calculating Top3 and Median')
    evidence = median_top3(evidence)

    logging.info('Start building Target and Diseases dataframe')
    target, diseases = Building_dataframe('targets', 'part-00000-9befc20b-ce53-4029-bd62-39c5b631aa3f-c000.json'),\
                Building_dataframe('diseases', 'part-00000-773deead-54e9-4934-b648-b26a4bbed763-c000.json')
    
    
    logging.info('Merging the 3 dataframes')
    dataframe_result = merge_data(evidence, diseases, target)

    logging.info('Filtering columns and sorting dataframe')
    dataframe_result = dataframe_result[['targetId', 'diseaseId', 'median', 'top3', 'approvedSymbol', 'name']]
    dataframe_result.sort_values(by=['median'], inplace=True)

    logging.info('Saving dataframe')
    dataframe_result.to_json(r'result.json')