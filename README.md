# python gridfs fuse
A couple of FUSE wrappers around MongoDB gridfs using python3 and pyfuse3.

This work is based on <https://github.com/axiros/py_gridfs_fuse>
and <https://github.com/Liam-Deacon/py_gridfs_fuse> developments.

There are two implementations:

* The classical one from [axiros](https://github.com/axiros/py_gridfs_fuse) and [Liam Deacon](https://github.com/Liam-Deacon/py_gridfs_fuse). It is a full filesystem, with subdirectories, but it is not compatible with existing GridFS collections.

* The naive one, done by me ([jmfernandez](https://github.com/jmfernandez/py_gridfs_fuse)). It is fully compatible with existing GridFS collections, but it is not able to hold subdirectories, and some write scenarios are not supported.

## Usage (classical)

```bash
gridfs_fuse --mongodb-uri="mongodb://127.0.0.1:27017" --database="gridfs_fuse" --mount-point="/mnt/gridfs_fuse" # --options=allow_other
```

### fstab example
```fstab
mongodb://127.0.0.1:27017/gridfs_fuse.fs  /mnt/gridfs_fuse  gridfs  defaults,allow_other  0  0 
```
Note this assumes that you have the `mount.gridfs` program (or `mount_gridfs` on MacOS X) symlinked 
into `/sbin/` e.g. `sudo ln -s $(which mount.gridfs) /sbin/`

## Usage (naive)

```bash
naive_gridfs_fuse --mongodb-uri="mongodb://127.0.0.1:27017" --database="gridfs_fuse" --mount-point="/mnt/gridfs_fuse" # --options=allow_other
```

### fstab example
```fstab
mongodb://127.0.0.1:27017/gridfs_fuse.fs  /mnt/gridfs_fuse  gridfs_naive  defaults,allow_other  0  0 
```
Note this assumes that you have the `mount.gridfs_naive` program (or `mount_gridfs_naive` on MacOS X) symlinked into `/sbin/` e.g. `sudo ln -s $(which mount.gridfs_naive) /sbin/`

## Requirements
 * pymongo
 * pyfuse3

## Install
Ubuntu 16.04:
```bash
sudo apt-get install libfuse python3-pip
sudo -H pip3 install git+https://github.com/jmfernandez/py_gridfs_fuse.git@v0.3.0
```

MacOSX:
```bash
brew install osxfuse
sudo -H pip3 install git+https://github.com/jmfernandez/py_gridfs_fuse.git@v0.3.0
```


## Operations supported (classical)
 * create/list/delete directories => folder support.
 * read files.
 * delete files.
 * open and write once (like HDFS).
 * rename

## Operations supported (naive)
 * list root directory.
 * read files.
 * delete files.
 * open and write once (like HDFS).
 * rename


## Operations not supported (classical)
 * modify an existing file.
 * resize an existing file.
 * hardlink
 * symlink
 * statfs

## Operations not supported (naive)
 * create/list/delete directories => folder support.
 * modify an existing file.
 * resize an existing file.
 * hardlink
 * symlink
 * statfs


## Performance (classical)
### Setup
* AWS d2.xlarge machine.
  * 4 @ 2.40Ghz (E5-2676)
  * 30 gigabyte RAM
* filesystem: ext4
* block device: three instance storage disks combined with lvm.
```
lvcreate -L 3T -n mongo -i 3 -I 4096 ax /dev/xvdb /dev/xvdc /dev/xvdd
```
* mongodb 3.0.1
* mongodb storage engine WiredTiger
* mongodb compression: snappy
* mongodb cache size: 10 gigabyte

### Results
* sequential write performance: ~46 MB/s
* sequential read performance: ~90 MB/s

Write performance was tested by copying 124 files, each having a size of 9 gigabytes and different content.
Compression factor was about factor three.
Files were copied one by one => no parallel execution.

Read performance was tested by randomly picking 10 files out of the 124.
Files were read one by one => no parallel execution.

```bash
# Simple illustration of the commands used (not the full script).

# Write
pv -pr /tmp/big_file${file_number} /mnt/gridfs_fuse/

# Read
pv -pr /mnt/gridfs_fuse${file_number} > /dev/null
```
