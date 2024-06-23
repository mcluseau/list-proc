./list-proc serve list.txt $* &
sleep 1
./list-proc process -- bash -c 'echo item: $item; sleep 0.2; test $item != jaw' &
./list-proc process -- bash -c 'echo item: $item; sleep 0.3; test $item != jaw'
