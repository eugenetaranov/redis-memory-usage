#### Optionally, setup local redis and sync data
- Create local redis
```shell
./main.py init
```

- Sync keys from
```shell
./main.py sync --src <src hostname|ip>
``` 

#### Generate report
```shell
./main.py report
```
#### Cleanup
```shell
./main.py cleanup
```
