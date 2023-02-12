from ftplib import FTP
from io import BytesIO
import pandas as pd
import logging
import statistics as st
import pyarrow.parquet as pq
import logging

def building_dataframe(data_type):
    """ Importing Parquet files using FTP """

    logging.info('Connecting to the URL')
    with FTP("ftp.ebi.ac.uk") as ftp:
        logging.info('establishing connection')
        ftp.login()

        logging.info('Cheking data_type')
        if data_type == 'evidence':
            ftp.cwd('pub/databases/opentargets/platform/21.11/output/etl/parquet/evidence/sourceId=eva/')
        else:
            ftp.cwd('pub/databases/opentargets/platform/21.11/output/etl/parquet/{}/'.format(data_type))

        logging.info('Start iterating on files')
        for file_name in ftp.nlst():
            if ".parquet" in file_name:
                r = BytesIO()

                logging.info('Reading file with the name {}'.format(file_name))
                ftp.retrbinary('RETR {}'.format(file_name), r.write)

                logging.info('reading parquet dataframe')
                if data_type == 'evidence':
                    df = pd.read_parquet(r, columns=['targetId', 'diseaseId', 'score'], engine='pyarrow')
                else:
                    df = pd.read_parquet(r, engine='pyarrow')
                
                logging.info('Linking dataframes')
                if 'part-00000' not in file_name:
                    dataframe = dataframe.append(df)
                else:
                    dataframe = df
        ftp.close()
        return (dataframe)


def median_top3(df_evidence):
    """Calculating the median and top three scores"""

    logging.info('Combining DiseaseId and TargetId')
    df_evidence['data'] = df_evidence['diseaseId'] + str('/') + df_evidence['targetId']

    logging.info('Filtering columns')
    df_evidence = df_evidence[['data', 'score']]

    logging.info('Grouping based on Column Data')
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
    """ Merging three dataframes: a diseases dataframe with the column diseaseId and a target dataframe with the column targetId """

    data = evidence.merge(diseases,left_on='diseaseId', right_on='id').merge(targets,left_on='targetId', right_on='id')
    return data

def number_target_target_disease(dataframe):
    """ Determine the number of targets who have at least two diseases in common """

    logging.info('Define counter')
    count = 0

    logging.info('Filtering Columns')
    dataframe2 = dataframe[['targetId', 'diseaseId']]

    logging.info('Get unique elements of TargetId')
    target_unique = dataframe2.targetId.unique().tolist()

    data_list = []

    # Each list define the disease that a target has
    logging.info('building a list contain lists')
    for element in target_unique:
        df = dataframe2[dataframe2["targetId"] == element] 
        col_list = df["diseaseId"].values.tolist()
        data_list.append(col_list)
    
    logging.info('Calculate counter')
    for i in range (len(data_list)-1):
        liste1 = data_list[i]
        liste2 = data_list[i+1]
        liste_total = liste1+liste2

        if  len(set(liste_total)) <= len(liste_total) - 2:
            count = count + 1
    
    return count



if "__main__" == __name__ :
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

    logging.info('Start building Evidence dataframe')
    evidence = building_dataframe('evidence')

    logging.info('Start Calculating Top3 and Median')
    evidence = median_top3(evidence)

    logging.info('Start building Target and Diseases dataframe')
    target, diseases = building_dataframe('targets'),\
                building_dataframe('diseases')
    
    
    logging.info('Merging the 3 dataframes')
    dataframe_result = merge_data(evidence, diseases, target)

    logging.info('Filtering columns and sorting dataframe')
    dataframe_result = dataframe_result[['targetId', 'diseaseId', 'median', 'top3', 'approvedSymbol', 'name']]

    logging.info('Sorting dataframe depending on median')
    dataframe_result.sort_values(by=['median'], inplace=True)

    logging.info('Reset dataframe Index')
    dataframe_result = dataframe_result.reset_index()

    logging.info('Saving dataframe')
    dataframe_result.to_json(r'result.json', orient='records')

    logging.info('The number of targets with at least two diseases in common is {}'.format(number_target_target_disease(dataframe_result)))
