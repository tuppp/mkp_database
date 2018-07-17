import os
#import importlib
import Load_file as lf
from os.path import isfile, join


def main():
    finished=[]
    try:
        finished_file = open('finished.txt', 'r')
        finished = finished_file.readlines()
        for i in range(len(finished)):
            if finished[i][len(finished[i])-1:] == '\n':
                finished[i] = finished[i][:len(finished[i])-1]
            finished_file.close()
    except FileNotFoundError:
        file = open('finished.txt', 'w')
        file.close()
    
    path_to_files = "/home/webcrawling/webscraping_2018/data"
    files = os.listdir(path_to_files)
    error_file = open('error.txt','w')
    for f in files:
        if f not in finished:
            if f[len(f)-3:] ==".py" or f == "finished.txt":
                continue
            count = 0
            i = 0
            tablename = "wettercom"
            for i in range(len(f)):
                if count == 3:
                    break
                if f[i] == "_" or f[i] == "-":
                    count = count+1
            if (f[i:] != 'wetter_com'):
                if(f[i:] == 'WETTERDIENST'):
                    tablename = 'wetterdienstde'
                else:
                    tablename = (f[i:])

            print('New File')
            print(tablename," ",path_to_files,"/",f)
            #ret = os.system("python MySql.py "+tablename+" "+f)
            ret =  lf.run(tablename,path_to_files+"/"+f)
            if ret:
                error_file.write(f+'\n')
                continue
            
            os.system("mv "+path_to_files+"/"+f+" /home/webcrawling/webscraping_2018/data/old")
if __name__ == "__main__":
    main()

