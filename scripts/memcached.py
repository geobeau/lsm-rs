from time import sleep
import bmemcached
import logging

logging.basicConfig(level=logging.DEBUG)


mc = bmemcached.Client(['127.0.0.1:11211'])

mc.socket_timeout = 60
print("connect")
mc.set("titi", "tutu")
sleep(1)
value = mc.get("titi")
print(value)
assert value == "tutu"
print("done")