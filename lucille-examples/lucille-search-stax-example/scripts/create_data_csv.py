from os import walk, listdir
from os.path import join, isfile
import pandas as pd

pnoa_path = 'data/PNOA_Sample'
pod_path = 'data/POD_Sample'

pnoa_files = [join(pnoa_path, f) for f in listdir(pnoa_path) if isfile(join(pnoa_path, f))]
pod_files = [join(pod_path, f) for f in listdir(pod_path) if isfile(join(pod_path, f))]

data_list = []
for file in pnoa_files:
    d = {
            'filename': file,
            'recordType': 'PNOA',
            'isPOC': 'True'
        }
    data_list.append(d)

for file in pod_files:
    d = {
            'filename': file,
            'recordType': 'POD',
            'isPOC': 'True'
        }
    data_list.append(d)


data = pd.DataFrame(data_list)
data.to_csv('conf/AER_data.csv', index=False)
