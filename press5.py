import os
import pandas as pd
from datetime import datetime

directory = '/home/ftp/press5'
files = os.listdir(directory)
wheels = pd.DataFrame(columns=['filename', 'date', 'time'])
i = 0
for x in files:
    string = datetime.fromtimestamp(os.path.getmtime(directory+'/'+x)).strftime('%Y-%m-%d %H:%M:%S').split(' ')
    wheels.loc[i] = [x, string[0], string[1]]   # через loc в несколько раз быстрее, чем через append
    i += 1
wheels.to_csv('/home/ftp/press5.csv')