Google File System 



Toy Example Test:

Terminal 1: Master
```
python master.py
```

Terminal 2: Chunkservers
```
mpiexec -n 4 python chunknew.py 5000 8888 chunkdump-5000
```

Terminal 3: Client
```
python client.py create /demo/file.txt 10
printf "contents of the file -- to be read later" | python client.py write /demo/file.txt
python client.py read /demo/file.txt
```
