# Flickr WARC to TFRecords

A script for extracting images and corresponding metadata from the [ArchiveTeam Flickr Dump](https://archive.org/details/archiveteam_flickr) and writing to TFRecords

# Compile instructions

```shell
RUSTFLAGS="-C target-cpu=native" cargo build --release
cp target/release/flickr_warc .
```

# Usage Instructions

```shell
# flickr_warc <input file> <output file>
flickr_warc flickr_20190324074003_89733133.megawarc.warc.gz flickr_20190324074003_89733133.tfrecords
```
