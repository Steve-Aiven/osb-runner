#!/usr/bin/env python3
"""Download the geonames corpus into the OSB local dataset cache."""
import configparser
import pathlib
import urllib.request

ini = pathlib.Path.home() / ".benchmark" / "benchmark.ini"
c = configparser.ConfigParser()
c.read(ini)
cache = pathlib.Path(
    c.get("benchmarks", "local.dataset.cache",
          fallback=str(pathlib.Path.home() / ".osb" / "benchmarks" / "data"))
)
print(f"OSB corpus cache: {cache}")

base_url = "https://dbyiw3u3rf9yr.cloudfront.net/corpora/geonames"
dest = cache / "geonames"
dest.mkdir(parents=True, exist_ok=True)

for filename in ["documents-2.json.bz2", "documents-2-1k.json.bz2"]:
    target = dest / filename
    print(f"Downloading {filename} -> {target}")
    urllib.request.urlretrieve(f"{base_url}/{filename}", target)
    print(f"  {target.stat().st_size / 1e6:.1f} MB")

print("Corpus pre-seed complete.")
