import os 

def get_files_paths():

    folderpaths = [
        # '/Users/hantswilliams/Downloads/trial/part0_2023/ClinicalTrials.2023-05-08.trials0',
        '/Users/hantswilliams/Downloads/trial/part1_2023/ClinicalTrials.2023-05-08.trials1',
        '/Users/hantswilliams/Downloads/trial/part2_2023/ClinicalTrials.2023-05-08.trials2',
        '/Users/hantswilliams/Downloads/trial/part3_2023/ClinicalTrials.2023-05-08.trials3',
        '/Users/hantswilliams/Downloads/trial/part4_2023/ClinicalTrials.2023-05-08.trials4',
        '/Users/hantswilliams/Downloads/trial/part5_2023/ClinicalTrials.2023-05-08.trials5',
    ]

    filenames = []

    for folderpath in folderpaths:
        print(folderpath)
        folders = os.listdir(folderpath)
        print(folders)
        for folder in folders:
            if folder != '.DS_Store':
                files = os.listdir(f'{folderpath}/{folder}')
                for i in range(len(files)):
                    filenames.append(f'{folderpath}/{folder}/{files[i]}')

    len(filenames) ## approximately 451k files

    return filenames