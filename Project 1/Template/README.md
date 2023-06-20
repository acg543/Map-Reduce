Test Machine: csel-atlas
Date: 02/09/2022
Name: Keean LaFerriere, [Alex Grenier]
x500: lafer011, [greni036]


Purpose of mapreduce.c:
    mapreduce.c takes in the number of mappers, number of reducers, and a text file as arguments and creates that amount of child mapper and reducer processes respectively. Then each mapper child process makes a system call execvp() to the "mapper" executable. The same is done for the reducer child processes.


How to Compile the Program:
    - Navigate to the Template Folder
    - type into the command line --- $ make t1


Outside Assumptions:
    N/A


Contributions of Team Members:
    Both team members worked evenly on mapreduce.c for the intermediate submission