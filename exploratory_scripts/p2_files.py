import os 

def get_files_paths():

    folderpaths = [
        # '/home/ubuntu/trial_data/ClinicalTrials.2023-05-08.trials0',
        '/home/ubuntu/trial_data/ClinicalTrials.2023-05-08.trials1',
        # '/home/ubuntu/trial_data/ClinicalTrials.2023-05-08.trials2',
        # '/home/ubuntu/trial_data/ClinicalTrials.2023-05-08.trials3',
        # '/home/ubuntu/trial_data/ClinicalTrials.2023-05-08.trials4',
        # '/home/ubuntu/trial_data/ClinicalTrials.2023-05-08.trials5',
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