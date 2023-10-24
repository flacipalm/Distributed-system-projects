python3 -u gentx.py 0.5 | python3 -u node.py node1 1234 configFile3/config1.txt > out1.txt &
python3 -u gentx.py 0.5 | python3 -u node.py node2 1235 configFile3/config2.txt > out2.txt &
python3 -u gentx.py 0.5 | python3 -u node.py node3 1236 configFile3/config3.txt > out3.txt &