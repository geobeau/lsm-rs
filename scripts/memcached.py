import bmemcached


mc = bmemcached.Client(['127.0.0.1:11211'])

print("connect")
mc.set("some_key", "Some value")
value = mc.get("some_key")
print("done")