import pandas as pd
import os
import re

"""changes format from group by strike price to group by expiration date"""

path = ("your path")
format  = ("end path")

class ExpDate:

    def __init__(self, df):
        self.df = df


expirations = [re.split("-",str(filename))[1] for filename in os.listdir(path)]
expirations = list(dict.fromkeys(expirations))

dict = {}

for filename in os.listdir(path):
    exp = re.split("-",str(filename))[1]
    df = pd.read_csv(path + "//" + filename)
    size = len(df)-1
    for acc in range(0,size):
        key = exp + "_" + str(acc)
        if key not in dict:
            col = df.columns
            dict[key] = ExpDate(pd.DataFrame(columns=col))
        temp = dict[key].df
        dict[key].df = temp.append(df.iloc[[acc]].fillna(0).reset_index())


for i in dict:
    with open(format + "//" + i +".csv",'w', newline='') as my_empty_csv:
        dict[i].df.to_csv(my_empty_csv)