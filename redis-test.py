import redis
from redis.cluster import RedisCluster

# r = redis.Redis(host='localhost', port=6379, decode_responses=True, protocol=3)
# r.set('foo', 'bar')
# assert r.get('foo') == 'bar'
# assert r.get('bob') is None


r = RedisCluster(host='localhost', port=6379,  protocol=3)
r.set('foo', 'bar')
assert r.get('foo') == b'bar'
assert r.get('bob') is None