import pandas as pd
import os
from tqdm import tqdm
import multiprocessing
from multiprocessing import Pool
from os import walk


import glob 
 
def walk_directory_to_list_files(path):
    fname = []
    for root,d_names,f_names in os.walk(path):
	    for f in f_names:
		    fname.append(os.path.join(root, f).replace('\\','/'))
    return fname
def create_commands_list(file_list,command,path_to_directory,bucket_link):
    commands = []
    for file in walk_directory_to_list_files(path_to_directory):
       # print(file)
        commands.append(command.format(file,os.path.join(bucket_link,file)))
    return commands

if __name__ == '__main__':
    command = 'aws s3 --only-show-errors cp {} {} --endpoint-url=https://s3.us-west-1.wasabisys.com'
    path_to_directory = 'C:/Users/sanjaymoto75/Downloads/lpr_data'
    os.chdir('/'.join(path_to_directory.split('/')[:-1]))
    #print(len(walk_directory_to_list_files(path_to_directory.split('/')[-1])))
    bucket_link = 's3://data-collection-ourteam/'
    commands = create_commands_list(walk_directory_to_list_files(path_to_directory.split('/')[-1]),command,path_to_directory.split('/')[-1],bucket_link)
    #print(commands[0])
    pool = Pool(30)
    list(tqdm(pool.imap(os.system, commands), total = len(commands)))
    pool.close()
    pool.join()
