
term1:
```
./list-proc serve list.txt list.state
```

term2:
```
./list-proc process -- bash -c 'echo item: $item; sleep 0.2; test $item != jaw'
```

term3:
```
./list-proc process -- bash -c 'echo item: $item; sleep 0.3; test $item != jaw'
```

