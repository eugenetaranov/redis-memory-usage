#!/usr/bin/env python
from typing import List, Any, Union

import redis
import docker
import humanize
import click


@click.group()
def cli():
    pass


class RunLocalRedis:
    def __init__(self, image: str = "redis", tag: str = "latest"):
        self.client = docker.from_env()
        self.image = image
        self.tag = tag
        self.container = None
        self.default_ports = {"6379/tcp": ("127.0.0.1", 6379)}
        self.labels = ["redis_memory_usage"]

    def run(self, ports=None):
        self.client.images.pull(repository=self.image, tag=self.tag)

        if ports is None:
            ports = self.default_ports
        self.container = self.client.containers.run(image=self.image, detach=True, ports=ports, labels=self.labels)

    def cleanup(self) -> bool:
        res = False
        containers = self.client.containers.list(filters={"label": self.labels})
        for c in containers:
            c.kill()
            c.remove()
            res = True

        return res

    def list_containers(self) -> List[str]:
        res = []
        containers = self.client.containers.list(filters={"label": self.labels})
        for c in containers:
            res.append(c.name)
        return res


class Redis:
    def __init__(self, host: str, port: int, db: int):
        self.host = host
        self.port = port
        self.db = db
        self.conn = redis.StrictRedis(host=self.host, port=self.port, db=self.db, charset="utf8")

    def list_keys(self, size: int = 0) -> iter:
        """
        lists first i or all keys
        :param size: how many keys to return, all if 0
        :return: list of key names
        """
        pos = 0
        end_pos = 0
        if size > 0:
            end_pos = size

        for k in self.conn.scan_iter():
            if end_pos != 0 and pos >= end_pos:
                break
            pos += 1
            key_name = k.decode("utf-8")
            yield key_name

    def get_memory_usage(self, key: str) -> int:
        return self.conn.memory_usage(key=key)

    def get_total_keys(self) -> int:
        return self.conn.info("keyspace")[f"db{self.db}"]["keys"]

    def __repr__(self):
        return repr(f"Host: {self.host}, port: {self.port}, db: {self.db}")


class RedisKey:
    def __init__(self, db: int, key: str, ttl: int, memory_usage: int):
        self.db = db
        self.key = key
        self.ttl = ttl
        self.memory_usage = memory_usage
        self.memory_usage_h = humanize.naturalsize(self.memory_usage)

    def __repr__(self):
        return repr(f"{self.db} {self.key} {self.memory_usage_h}")


def migrate(src: Redis, dst: Redis):
    cursor = 0
    batch_size = 1000
    total_keys = src.conn.info("keyspace")[f"db{src.db}"]["keys"]
    with click.progressbar(length=total_keys, label=f"db{src.db}") as bar:
        while True:
            cursor, keys = src.conn.scan(cursor, count=batch_size)
            src_pipeline = src.conn.pipeline(transaction=False)
            for key in keys:
                src_pipeline.pttl(key)
                src_pipeline.dump(key)
            dumps = src_pipeline.execute()

            dst_pipeline = dst.conn.pipeline(transaction=False)
            for key, ttl, data in zip(keys, dumps[::2], dumps[1::2]):
                if data is not None:
                    dst_pipeline.restore(key, ttl if ttl > 0 else 0, data, replace=True)

            results = dst_pipeline.execute(False)
            for key, result in zip(keys, results):
                if result not in (b"OK", b"BUSYKEY Target key name already exists."):
                    raise Exception("Migration failed on key {}: {}".format(key, result))

            bar.update(batch_size)
            if cursor == 0:
                break


@cli.command()
def init():
    local_redis = RunLocalRedis()
    local_redis.run()
    print(f"Started local redis")


@cli.command()
def cleanup():
    local_redis = RunLocalRedis()
    res = local_redis.cleanup()
    if res:
        print(f"Terminated local redis")


@cli.command()
@click.option("--src", help="Source redis uri host:[port]:[db]")
@click.option("--dst", default="127.0.0.1:6379", help="Destination redis uri host:[port]:[db]")
def check(src, dst):
    local_redis = RunLocalRedis()
    containers = local_redis.list_containers()
    if containers:
        click.secho(f"Found the following containers: {containers}", fg="green")

        src_parsed = parse_uri(src)
        dst_parsed = parse_uri(dst)
        active_dbs_list = []

        # discover databases
        if src_parsed["db"] == -1 or dst_parsed["db"] == -1:
            src = Redis(host=src_parsed["host"], port=src_parsed["port"], db=0)

            for k in src.conn.info("keyspace").keys():
                active_dbs_list.append(int(k.replace("db", "")))
        else:
            active_dbs_list = [src_parsed["db"]]

        for db in active_dbs_list:
            src = Redis(host=src_parsed["host"], port=src_parsed["port"], db=db)
            dst = Redis(host=dst_parsed["host"], port=dst_parsed["port"], db=db)
            check_keys(src, dst, db)
    else:
        click.secho(f"No local containers were found", fg="red")


def check_keys(src: Redis, dst: Redis, db: int):
    src_keys = src.conn.info("keyspace")[f"db{db}"]["keys"]
    try:
        dst_keys = dst.conn.info("keyspace")[f"db{db}"]["keys"]
    except KeyError:
        click.secho(
            f"Keyspace {db} does not exist in destination redis",
            fg="red")
        return

    if src_keys == dst_keys:
        click.secho(
            f"Keyspace {db} is in sync, source redis has {src_keys} keys, destination redis has {dst_keys} keys",
            fg="green")
    else:
        click.secho(
            f"Keyspace {db} is not in sync, source redis has {src_keys} keys, destination redis has {dst_keys} keys",
            fg="red")


@cli.command()
@click.option("--src", help="Source redis uri host:[port]:[db]")
@click.option("--dst", default="127.0.0.1:6379", help="Destination redis uri host:[port]:[db]")
def sync(src: str, dst: str):
    src_parsed = parse_uri(src)
    dst_parsed = parse_uri(dst)
    active_dbs_list = []

    # discover databases
    if src_parsed["db"] == -1 or dst_parsed["db"] == -1:
        src = Redis(host=src_parsed["host"], port=src_parsed["port"], db=0)

        for k in src.conn.info("keyspace").keys():
            active_dbs_list.append(int(k.replace("db", "")))
    else:
        active_dbs_list = [src_parsed["db"]]

    # migrate data
    click.echo(f"Syncing...")
    for db in active_dbs_list:
        src = Redis(host=src_parsed["host"], port=src_parsed["port"], db=db)
        dst = Redis(host=dst_parsed["host"], port=dst_parsed["port"], db=db)
        dst.conn.flushdb(asynchronous=False)
        migrate(src=src, dst=dst)

        # validate
        check_keys(src=src, dst=dst, db=db)


def parse_uri(uri: str) -> dict:
    res = {}
    l = uri.split(":")
    res["host"] = l[0]
    if len(l) >= 2:
        res["port"] = int(l[1])
    else:
        res["port"] = 6379

    if len(l) >= 3:
        res["db"] = int(l[2])
    else:
        res["db"] = -1

    return res


@cli.command()
@click.option("--dst", default="127.0.0.1:6379", help="Destination redis uri host:[port]:[db]")
def report(dst):
    total_keys = 0
    total_memory_usage = 0
    keys_no_ttl = 0
    # biggest_key = {"key": None, "memory_usage": 0, "ttl": 0}
    top_biggest_keys = []
    active_dbs_list = []
    dst_parsed = parse_uri(dst)

    # discover databases
    if dst_parsed["db"] == -1:
        dst = Redis(host=dst_parsed["host"], port=dst_parsed["port"], db=0)

        for k in dst.conn.info("keyspace").keys():
            active_dbs_list.append(int(k.replace("db", "")))
    else:
        active_dbs_list = [dst_parsed["db"]]

    # get memory stats
    print(f"Scanning...")
    for db in active_dbs_list:
        dst = Redis(host=dst_parsed["host"], port=dst_parsed["port"], db=db)

        with click.progressbar(length=dst.get_total_keys(), label=f"db{db}") as bar:
            for k in dst.list_keys():
                memory_usage = dst.get_memory_usage(k)
                ttl = dst.conn.ttl(k)

                total_keys += 1
                total_memory_usage += memory_usage

                if ttl == -1:
                    keys_no_ttl += 1

                # filling in top_biggest_keys
                if len(top_biggest_keys) < 10:
                    top_biggest_keys.append(RedisKey(db=db, key=k, ttl=ttl, memory_usage=memory_usage))
                    top_biggest_keys = sorted(top_biggest_keys, key=lambda x: x.memory_usage, reverse=True)
                else:
                    if memory_usage > top_biggest_keys[-1].memory_usage:
                        top_biggest_keys.pop(-1)
                        top_biggest_keys.append(RedisKey(db=db, key=k, ttl=ttl, memory_usage=memory_usage))
                        top_biggest_keys = sorted(top_biggest_keys, key=lambda x: x.memory_usage, reverse=True)

                bar.update(1)

    click.secho(f"Top keys:", fg="green")
    for k in top_biggest_keys:
        click.echo(f"{k.db} {k.key} {click.style(k.memory_usage_h, fg='red')}")
    click.secho(
        f"Total keys: {total_keys}, total memory: {humanize.naturalsize(total_memory_usage)}, keys without ttl: {keys_no_ttl}",
        fg="green")


if __name__ == "__main__":
    cli()
