import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True, protocol=3)
r.set('foo', 'bar')
assert r.get('foo') == 'bar'
assert r.get('bob') is None
