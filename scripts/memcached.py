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
mc.set("foo", "bar")
value = mc.get("foo")
print(value)
assert value == "bar"


value = mc.get("test1")
value = mc.get("test2")
value = mc.get("test3")
value = mc.get("test4")
print("done")
