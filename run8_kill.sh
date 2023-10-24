python3 -u gentx.py 0.5 | python3 -u node.py node1 1234 configFile8/config1.txt > out1.txt &
pid1=$!

python3 -u gentx.py 0.5 | python3 -u node.py node2 1235 configFile8/config2.txt > out2.txt &
pid2=$!

python3 -u gentx.py 0.5 | python3 -u node.py node3 1236 configFile8/config3.txt > out3.txt &
pid3=$!

python3 -u gentx.py 0.5 | python3 -u node.py node4 1237 configFile8/config4.txt > out4.txt &
pid4=$!

python3 -u gentx.py 0.5 | python3 -u node.py node5 1238 configFile8/config5.txt > out5.txt &
pid5=$!

python3 -u gentx.py 0.5 | python3 -u node.py node6 1239 configFile8/config6.txt > out6.txt &
pid6=$!

python3 -u gentx.py 0.5 | python3 -u node.py node7 1240 configFile8/config7.txt > out7.txt &
pid7=$!

python3 -u gentx.py 0.5 | python3 -u node.py node8 1241 configFile8/config8.txt > out8.txt &
pid8=$!

sleep 100s
kill $pid1
kill $pid2
kill $pid3
echo "node 1, 2, 3 failed"

sleep 100s
kill $pid4
kill $pid5
kill $pid6
kill $pid7
kill $pid8
echo "finished"