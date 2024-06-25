./list-proc serve $* list.txt -- bash -c 'echo item: $item; sleep 0.2; test $item != jaw' &
sleep 1
./list-proc process &
./list-proc process
