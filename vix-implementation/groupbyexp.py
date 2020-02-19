import pandas as pd
import os
import re
import csv

"""changes format from group by strike price to group by expiration date"""

path = (r"C:\Users\ggoh\Documents\bitFear\scrapped")
format  = (r"C:\Users\ggoh\Documents\bitFear\temp")




expirations = [re.split("-",str(filename))[1] for filename in os.listdir(path)]
expirations = list(dict.fromkeys(expirations))

dict = {}

for filename in os.listdir(path):
    exp = re.split("-",str(filename))[1]
    with open(path + "//" + filename) as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)
        acc = 0
        for row in reader:
            key = exp + "_" + str(acc)
            if key not in dict:
                dict[key] = [header]
            dict[key] = dict[key] + [row]
            acc += 1

for i in dict:
    with open(format + "//" + i +".csv",'w', newline='') as my_empty_csv:
        writer = csv.writer(my_empty_csv, delimiter=',')
        for row in dict[i]:
            writer.writerow(row)