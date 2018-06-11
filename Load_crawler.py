import os
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

    files = os.listdir()
    print(files)

    finished_file = open('finished.txt', 'a')
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
                if(f[i:] == WETTERDIENST):
                    tablename = 'wetterdienstde'
                else:
                    tablename = (f[i:])

            print('New File')
            if os.system("python MySql.py "+tablename+" "+f):
                continue
            finished.append(f)
            finished_file.write(f+'\n')
    finished_file.close()
if __name__ == "__main__":
    main()

