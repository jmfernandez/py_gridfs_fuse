import logging
import os
import stat
import time
import errno
import collections
import itertools

import pyfuse3
import gridfs

import pymongo
from .pymongo_compat import compat_collection

from distutils.version import LooseVersion


mask = stat.S_IWGRP | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH


RETRY_WRITES_MIN_VERSION = LooseVersion("3.6")


def grid_in_size(grid_in):
    return grid_in._position + grid_in._buffer.tell()


try:
    time_ns = time.time_ns
except:
    import ctypes

    CLOCK_REALTIME = 0

    class timespec(ctypes.Structure):
        _fields_ = [
            ('tv_sec', ctypes.c_int64), # seconds, https://stackoverflow.com/q/471248/1672565
            ('tv_nsec', ctypes.c_int64), # nanoseconds
            ]

    clock_gettime = ctypes.cdll.LoadLibrary('libc.so.6').clock_gettime
    clock_gettime.argtypes = [ctypes.c_int64, ctypes.POINTER(timespec)]
    clock_gettime.restype = ctypes.c_int64

    def time_ns():
        tmp = timespec()
        ret = clock_gettime(CLOCK_REALTIME, ctypes.pointer(tmp))
        if bool(ret):
            raise OSError()
        return tmp.tv_sec * 10 ** 9 + tmp.tv_nsec


class Entry(object):
    def __init__(self, ops, filename, inode, parent_inode, mode, uid, gid):
        self._ops = ops

        self._id = inode
        self.filename = filename
        self.parent_inode = parent_inode
        self.mode = mode
        self.uid = uid
        self.gid = gid

        self.atime_ns = self.mtime_ns = self.ctime_ns = time_ns()

        # Only for directories
        # filename: inode
        self.childs = {}

    @property
    def inode(self):
        return self._id


class Operations(pyfuse3.Operations):
    def __init__(self, database, collection='fs', logfile=None, debug=os.environ.get('GRIDFS_FUSE_DEBUG'), filename_encoding='utf-8'):
        super(Operations, self).__init__()

        self.logger = logging.getLogger("gridfs_fuse")
        self.logger.setLevel(logging.DEBUG if debug else logging.ERROR)
        try:
            self.handler = logging.FileHandler(logfile)
            self.handler.setLevel(logging.DEBUG)
        except:
            pass
        #self._readonly = read_only
        self._database = database
        self._collection = collection
        self._filename_encoding = filename_encoding
        
        self.meta = compat_collection(database, collection + '.metadata')
        self.gridfs = gridfs.GridFS(database, collection)
        self.gridfs_files = compat_collection(database, collection + '.files')

        self.active_inodes = collections.defaultdict(int)
        self.active_writes = {}

    async def open(self, inode, flags, ctx):
        self.logger.debug("open: %s %s", inode, flags)

        # Do not allow writes to a existing file
        if flags & os.O_WRONLY: 
            raise pyfuse3.FUSEError(errno.EACCES)

        # Deny if write mode and filesystem is mounted as read-only
        #if flags & (os.O_RDWR | os.O_CREAT | os.O_WRONLY | os.O_APPEND) and self._readonly:
        #    raise pyfuse3.FUSWERROR(errno.EPERM)
        
        self.active_inodes[inode] += 1
        return pyfuse3.FileInfo(fh=inode)

    async def opendir(self, inode, ctx):
        """Just to check access, dont care about access => return inode"""
        self.logger.debug("opendir: %s", inode)
        return inode

    async def access(self, inode, mode, ctx):
        """Again this fs does not care about access"""
        self.logger.debug("access: %s %s %s", inode, mode, ctx)
        return True

    async def getattr(self, inode, ctx=None):
        self.logger.debug("getattr: %s", inode)
        return await self._gen_attr(self._entry_by_inode(inode))

    async def readdir(self, inode, off, token):
        self.logger.debug("readdir: %s %s", inode, off)

        entry = self._entry_by_inode(inode)
        for index, child_inode in enumerate(itertools.islice(entry.childs.values(),off,None),off+1):
            child = self._entry_by_inode(child_inode)
            
            if not pyfuse3.readdir_reply(token,child.filename.encode(self._filename_encoding), await self._gen_attr(child), index):
                break
        return

    async def lookup(self, folder_inode, bname, ctx):
        self.logger.debug("lookup: %s %s", folder_inode, bname)
        
        # Names are in bytes, so translate to UTF-8
        name = bname.decode(self._filename_encoding,'replace')
        
        if name == '.':
            inode = folder_inode

        elif name == '..':
            entry = self._entry_by_inode(folder_inode)
            inode = entry.parent_inode

        else:
            entry = self._entry_by_inode(folder_inode)
            for fn, inode in entry.childs.items():
                if fn == name:
                    break
            else:
                raise pyfuse3.FUSEError(errno.ENOENT)

        return await self.getattr(inode)

    async def mknod(self, inode_p, bname, mode, rdev, ctx):
        self.logger.debug("mknod")
        raise pyfuse3.FUSEError(errno.ENOSYS)

    async def mkdir(self, folder_inode, bname, mode, ctx):
        self.logger.debug("mkdir: %s %s %s %s", folder_inode, bname, mode, ctx)
        entry = await self._create_entry(folder_inode, bname, mode, ctx)
        return await self._gen_attr(entry)

    async def create(self, folder_inode, bname, mode, flags, ctx):
        self.logger.debug("create: %s %s %s %s", folder_inode, bname, mode, flags)

        entry = await self._create_entry(folder_inode, bname, mode, ctx)
        grid_in = self._create_grid_in(entry)

        self.active_inodes[entry.inode] += 1
        self.active_writes[entry.inode] = grid_in

        return (pyfuse3.FileInfo(fh=entry.inode), await self._gen_attr(entry))

    def _create_grid_in(self, entry):
        gridfs_filename = self._create_full_path(entry)
        return self.gridfs.new_file(_id=entry.inode, filename=gridfs_filename)

    def _create_full_path(self, entry):
        # Build the full path for this file.
        # Add the full path to make other tools like
        # mongofiles, mod_gridfs, ngx_gridfs happy
        path = collections.deque()
        while entry._id != pyfuse3.ROOT_INODE:
            path.appendleft(entry.filename)
            entry = self._entry_by_inode(entry.parent_inode)
        path.appendleft(entry.filename)        
        return os.path.join(*path)

    async def _create_entry(self, folder_inode, bname, mode, ctx):
        inode = self._gen_inode()
        # Names are in bytes, so translate to UTF-8
        name = bname.decode(self._filename_encoding,'replace')
        entry = Entry(self, name, inode, folder_inode, mode, ctx.uid, ctx.gid)

        self._insert_entry(entry)

        query = {"_id":  folder_inode}
        update = {"$addToSet": {"childs": (name, inode)}}
        self.meta.update_one(query, update)

        return entry
    
    Fields2Attr = {
        'update_size': None,
        'update_uid': 'st_uid',
        'update_gid': 'st_gid',
        'update_mode': 'st_mode',
        'update_atime': 'st_atime_ns',
        'update_mtime': 'st_mtime_ns',
        'update_ctime': 'st_ctime_ns',
    }
    
    async def setattr(self, inode, attr, fields, fh, ctx):
        self.logger.debug("setattr: %s %s", inode, attr)

        #if attr.st_rdev:
        #    raise pyfuse3.FUSEError(errno.ENOSYS)
        
        if fields.update_size:
            raise pyfuse3.FUSEError(errno.EINVAL)
        
        entry = self._entry_by_inode(inode)

        # No way to change the size of an existing file.
        for field_name, attr_name in self.Fields2Attr.items():
            if getattr(fields,field_name,None):
                val = getattr(attr, attr_name, None)
                if val is not None:
                    target = attr_name[3:]
                    setattr(entry, target, val)

        self._update_entry(entry)
        return await self.getattr(inode,ctx)

    async def unlink(self, folder_inode, bname, ctx):
        self.logger.debug("unlink: %s %s", folder_inode, bname)

        self._delete_inode(
            folder_inode,
            bname,
            self._delete_inode_check_file)

    async def rmdir(self, folder_inode, bname, ctx):
        self.logger.debug("rmdir: %s %s", folder_inode, bname)

        self._delete_inode(
            folder_inode,
            bname,
            self._delete_inode_check_directory)

    def _delete_inode(self, folder_inode, bname, entry_check):
        # On insert the order is like this
        # 1. write into the database.
        #    the unique index (parent_inode, filename) protects
        # 2. Update the folder inode

        # On remove the order must be vice verca
        # 1. Remove from the folder inode
        # 2. Remove from the database

        # In that case the unique index protection is true

        # Names are in bytes, so translate to UTF-8
        name = bname.decode(self._filename_encoding,'replace')
        
        parent = self._entry_by_inode(folder_inode)

        if name not in parent.childs:
            raise pyfuse3.FUSEError(errno.ENOENT)
        inode = parent.childs[name]

        entry = self._entry_by_inode(inode)
        entry_check(entry)

        # Remove from the folder node
        query = {"_id": folder_inode}
        update = {"$pull": {'childs': (name, inode)}}
        self.meta.update_one(query, update)

        # Remove from the database
        self.meta.delete_one({"_id": inode})

        # Remove from the grids collections
        self.gridfs.delete(inode)

    def _delete_inode_check_file(self, entry):
        if stat.S_ISDIR(entry.mode):
            raise pyfuse3.FUSEError(errno.EISDIR)

    def _delete_inode_check_directory(self, entry):
        if not stat.S_ISDIR(entry.mode):
            raise pyfuse3.FUSEError(errno.ENOTDIR)

        if len(entry.childs) > 0:
            raise pyfuse3.FUSEError(errno.ENOTEMPTY)

    async def read(self, inode, offset, length):
        self.logger.debug("read: %s %s %s", inode, offset, length)

        try:
            grid_out = self.gridfs.get(inode)
        except gridfs.errors.NoFile:
            msg = "Read of inode (%s) fails. Gridfs object not found"
            self.logger.error(msg, inode)
            raise pyfuse3.FUSEError(errno.EIO)

        grid_out.seek(offset)
        return grid_out.read(length)

    async def write(self, inode, offset, data):
        self.logger.debug("write: %s %s %s", inode, offset, len(data))

        # Only 'append once' semantics are supported.

        if inode not in self.active_writes:
            raise pyfuse3.FUSEError(errno.EINVAL)

        grid_in = self.active_writes[inode]

        if offset != grid_in_size(grid_in):
            raise pyfuse3.FUSEError(errno.EINVAL)

        grid_in.write(data)
        return len(data)

    async def release(self, inode):
        self.logger.debug("release: %s", inode)

        self.active_inodes[inode] -= 1
        if self.active_inodes[inode] == 0:
            del self.active_inodes[inode]
            if inode in self.active_writes:
                self.active_writes[inode].close()
                del self.active_writes[inode]

    async def releasedir(self, inode):
        self.logger.debug("releasedir: %s", inode)

    async def forget(self, inode_list):
        for inode, nlookup in inode_list:
            if inode in self.active_inodes:
                self.active_inodes[inode] -= nlookup
                if self.active_inodes[inode] <= 0:
                    del self.active_inodes[inode]
                    if inode in self.active_writes:
                        self.active_writes[inode].close()
                        del self.active_writes[inode]
        
        self.logger.debug("forget: %s", inode_list)

    async def readlink(self, inode, ctx):
        self.logger.debug("readlink: %s", inode)
        raise pyfuse3.FUSEError(errno.ENOSYS)

    async def symlink(self, folder_inode, bname, target, ctx):
        self.logger.debug("symlink: %s %s %s", folder_inode, bname, target)
        raise pyfuse3.FUSEError(errno.ENOSYS)

    async def rename(self, old_folder_inode, old_bname, new_folder_inode, new_bname, flags, ctx):
        self.logger.debug(
            "rename: %s %s %s %s",
            old_folder_inode,
            old_bname,
            new_folder_inode,
            new_bname)

        # Load the entry to move
        entry_attributes = await self.lookup(old_folder_inode, old_bname, ctx)
        entry = self._entry_by_inode(entry_attributes.st_ino)

        # Load the target directory
        new_folder = self._entry_by_inode(new_folder_inode)

        # Check if the folder already contains this name and remove it.
        
        # Names are in bytes, so translate to UTF-8
        new_name = new_bname.decode(self._filename_encoding,'replace')
        
        if new_name in new_folder.childs:
            noop = lambda entry: None
            self._delete_inode(new_folder.inode, new_bname, noop)

        # Set the new parent and filename to the existing inode.
        query = {"_id": entry.inode}
        update = {
            "$set": {
                'parent_inode': new_folder.inode,
                'filename': new_name
            }
        }
        self.meta.update_one(query, update)

        # Erase the inode from the older folder.
        query = {"_id": old_folder_inode}
        update = {"$pull": {'childs': (entry.filename, entry.inode)}}
        self.meta.update_one(query, update)

        # Add the inode to the new folder
        query = {"_id": new_folder.inode}
        update = {"$addToSet": {'childs': (new_name, entry.inode)}}
        self.meta.update_one(query, update)

        # Ensure the correct filename within gridfs
        entry.parent_inode = new_folder.inode
        entry.filename = new_name

        gridfs_filename = self._create_full_path(entry)
        query = {"_id": entry.inode}
        update = {"$set": {'filename': gridfs_filename}}
        self.gridfs_files.update_one(query, update)

    async def link(self, inode, new_parent_inode, new_bname, ctx):
        self.logger.debug("link: %s %s %s", inode, new_parent_inode, new_bname)
        raise pyfuse3.FUSEError(errno.ENOSYS)

    async def flush(self, fd):
        self.logger.debug("flush: %s", fd)
        raise pyfuse3.FUSEError(errno.ENOSYS)

    async def fsync(self, fd, datasync):
        self.logger.debug("fsync: %s %s", fd, datasync)
        raise pyfuse3.FUSEError(errno.ENOSYS)

    async def fsyncdir(self, fd, datasync):
        self.logger.debug("fsyncdir: %s %s", fd, datasync)
        raise pyfuse3.FUSEError(errno.ENOSYS)

    async def statfs(self, ctx):
        self.logger.debug("statfs")
        raise pyfuse3.FUSEError(errno.ENOSYS)

    def _entry_by_inode(self, inode):
        query = {'_id': inode}
        record = self.meta.find_one(query)
        return self._doc_to_entry(record or {'childs': []})

    def _insert_entry(self, entry):
        doc = self._entry_to_doc(entry)
        self.meta.insert_one(doc)

    def _update_entry(self, entry):
        query = {"_id": entry.inode}
        doc = self._entry_to_doc(entry)
        self.meta.update_one(query, {"$set": doc})

    def _entry_to_doc(self, entry):
        doc = dict(vars(entry))
        del doc['_ops']
        doc['childs'] = list(entry.childs.items())
        return doc

    def _doc_to_entry(self, doc):
        doc['_ops'] = self
        doc['childs'] = dict(doc['childs'])
        entry = object.__new__(Entry)
        entry.__dict__.update(doc)
        return entry

    async def _gen_attr(self, entry):
        attr = pyfuse3.EntryAttributes()

        attr.st_ino = entry.inode
        attr.generation = 0
        attr.entry_timeout = 10
        attr.attr_timeout = 10

        attr.st_mode = entry.mode
        attr.st_nlink = 1

        attr.st_uid = entry.uid
        attr.st_gid = entry.gid
        attr.st_rdev = 0

        attr.st_size = self._get_entry_size(entry)

        attr.st_blksize = 512
        attr.st_blocks = (attr.st_size // attr.st_blksize) + 1

        attr.st_atime_ns = entry.atime_ns
        attr.st_mtime_ns = entry.mtime_ns
        attr.st_ctime_ns = entry.ctime_ns

        return attr

    def _get_entry_size(self, entry):
        if stat.S_ISDIR(entry.mode):
            return 4096

        if entry.inode in self.active_writes:
            return grid_in_size(self.active_writes[entry.inode])

        # pymongo creates the entry only when the file is completely written
        # and *closed* by the writer.
        # => As long as the file is written (not closed) 'self.gridfs.get'
        # returns an ERROR on other nodes doing a 'get'.
        # This happens on other nodes *not* doing the actual write.
        # The node doing the write has the current file-object in memory
        # (self.active_writes).
        # => As long as the file is written, other nodes see only size=0
        try:
            return self.gridfs.get(entry._id).length
        except gridfs.errors.NoFile:
            return 0

    def _gen_inode(self):
        query = {"_id": "next_inode"}
        update = {"$inc": {"value": 1}}
        doc = self.meta.find_one_and_update(query, update)
        return doc['value']


def _ensure_root_inode(ops):
    root = Entry(
        ops,
        '/',
        pyfuse3.ROOT_INODE,
        pyfuse3.ROOT_INODE,
        stat.S_IFDIR | stat.S_IRWXU | mask,
        os.getuid(),
        os.getgid())

    try:
        ops._insert_entry(root)
    except pymongo.errors.DuplicateKeyError:
        pass


def _ensure_next_inode_document(ops):
    # Use this document to create inodes
    try:
        ops.meta.insert_one({
            "_id": "next_inode",
            'value': pyfuse3.ROOT_INODE + 1
        })
    except pymongo.errors.DuplicateKeyError:
        pass


def _ensure_indexes(ops):
    # Use this index to ensure that now 'duplicate' documents
    # are in the same folder
    index = [
        ('parent_inode', pymongo.ASCENDING),
        ('filename', pymongo.ASCENDING)
    ]
    try:
        ops.meta.create_index(index, unique=True)
    except pymongo.errors.OperationFailure:
        ops.meta.drop()
        _ensure_root_inode(ops)
        _ensure_next_inode_document(ops)
        ops.meta = compat_collection(ops._database, ops._collection + '.metadata')
        ops.meta.create_index(index, unique=False)

DEFAULT_WIRE_VERSION = '3.6.0'
WireVersion2Version={
    3: '2.6.0',
    4: '3.2.0',
    5: '3.4.0',
    6: DEFAULT_WIRE_VERSION,
    7: '4.0.0',
    8: '4.2.0',
    9: '4.4.0',
}

def get_compat_version(client):
    # This is needed to avoid calling an administrative command
    if hasattr(client,'_server_property'):
        max_wire_version = client._server_property("max_wire_version")
        compat_version = WireVersion2Version.get(max_wire_version,DEFAULT_WIRE_VERSION)
    else:
        compat_cmd = {"getParameter": 1, "featureCompatibilityVersion": 1}
        cmd_response = client.admin.command(compat_cmd)
        compat_version = cmd_response["featureCompatibilityVersion"]
    
    if "version" in compat_version:
        compat_version = compat_version["version"]

    return LooseVersion(compat_version)


def operations_factory(options):
    logger = logging.getLogger("gridfs_fuse")

    old_pymongo = LooseVersion(pymongo.version) < LooseVersion("3.6.0")

    if old_pymongo:
        client = pymongo.MongoClient(options.mongodb_uri)
    else:
        client = pymongo.MongoClient(options.mongodb_uri, retryWrites=True)

    compat_version = get_compat_version(client)
    if old_pymongo or compat_version < RETRY_WRITES_MIN_VERSION:
        logger.warning(
                "Your featureCompatibilityVersion (%s) is lower than the "
                "required %s for retryable writes to work. "
                "Due to this file operations might fail if failovers happen."
                "Additionally, this feature requires pymongo >= 3.6.0 "
                "(Yours: %s).",
                compat_version,
                RETRY_WRITES_MIN_VERSION,
                pymongo.version)

    ops = Operations(client[options.database], collection=options.collection, logfile=options.logfile)
    _ensure_root_inode(ops)
    _ensure_next_inode_document(ops)
    _ensure_indexes(ops)

    return ops
