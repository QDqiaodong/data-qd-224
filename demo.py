from pathlib import Path
from tempfile import TemporaryDirectory
from objects import ObjectStore

with TemporaryDirectory() as root:
    path = Path(root, "cache.db")
    cache = ObjectStore(path)
    a = cache.publish("stable", b"artifact-v1")
    b = cache.publish("tested", b"artifact-v1")
    print("same digest:", a == b)
    cache.publish("stable", b"artifact-v2")
    cache = ObjectStore(path)
    print("both readable:", cache.fetch("stable"), cache.fetch("tested"))
    print("still referenced:", cache.collect())
    cache.remove("tested")
    print("reclaimed:", cache.collect())
    print("stable:", cache.fetch("stable"))
