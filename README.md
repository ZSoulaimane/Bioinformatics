# Bioinformatics

## files

        - data_transformation_FTP to generate result using FTP as a source of data
        - data_transformation to generate result using file downloaded as a source of data

## libraries used

#### Pandas

```bash
pip install pandas
```
#### statistics

```bash
pip install statistics
```

## Function Definition

    * Building_dataframe is a function that take 2 input to build a general dataframe from all the json files in diseases, evidence and target

    * median_top3 the main purpose of this function is to calculate median and Top3 scores of each target

    * merge_data this function is to merge between 3 dataframes (Disease, Target, evidence)

    * number_target_target_disease this function is to calculate the number of targets that share atleast 2 diseases

## Clarification
    in Function number_target_target_disease the data_list contain lists each define the target-diseases lists

            |----- Disease 1
    target1 |
            |----- Disease 2
            |----- Disease 3

            |----- Disease 1
    target2 |
            |----- Disease 4
            |----- Disease 3
    
    - the data_list will contain two lists [[Disease1, Disease2, Disease3], [Disease1, Disease4, Disease3]]
    
    - Target_unique list contain the unique TargetIds of result dataframe

    data_list = [[Disease1, Disease2, Disease3], [Disease1, Disease4, Disease3]]

    index of target1 in "target_unique" list is the same index for "data_list" diseases

    example : 

    target_unique = [target1, target2]

    target_unique[0] = target1

    disease that target1 connect to are : data_list[0]

    target_unique[0] ---> data_list[0]

    Same for target2
