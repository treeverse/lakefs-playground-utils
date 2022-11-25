import datetime
import os
import tempfile
from typing import List, Tuple, Union

from fsspec import register_implementation
from fsspec.spec import AbstractFileSystem, AbstractBufferedFile

import lakefs_client
from lakefs_client.client import LakeFSClient


def _split_path(path: str) -> Tuple[str, str, str]:
    repo, _, rest = path.partition("/")
    ref, _, rest = rest.partition("/")
    return repo, ref, rest


def _remove_suffix(path: str, suffix: str) -> str:
    return path if not path.endswith(suffix) else path[:len(path) - len(suffix)]


def _object_stat_to_entry(repo, ref, stat):
    path = stat.get("path")
    key = f"{repo}/{ref}/{path}"
    if stat.get("path_type") == "object":
        return {
            "ETag": stat.get("checksum"),
            "Key": key,
            "name": key,
            "type": "file",
            "size": stat.get("size_bytes"),
            "Size": stat.get("size_bytes"),
            "StorageClass": "STANDARD",
            "LastModified": datetime.datetime.fromtimestamp(
                stat.get("mtime"), datetime.timezone.utc
            ),
        }
    return {
        "Key": _remove_suffix(key, "/"),
        "name": _remove_suffix(key, "/"),
        "size": 0,
        "Size": 0,
        "StorageClass": "DIRECTORY",
        "type": "directory",
    }


class LakeFSNativeFS(AbstractFileSystem):
    def __init__(self, key, secret, host, *args, **kwargs):
        self.key = key
        self.secret = secret
        self.host = host
        super().__init__(*args, **kwargs)
        # init client
        self._client_configuration = lakefs_client.Configuration(
            host=f"https://{host}/api/v1", username=key, password=secret
        )
        self._client = LakeFSClient(self._client_configuration)

    def ls(self, path, detail=True, **kwargs):
        repo, ref, key = _split_path(path)
        return self._ls(repo, ref, key, detail, **kwargs)

    def _ls(self, repo, ref, key, detail=True, **kwargs):
        records = []
        after = None
        while True:
            kwargs = {"prefix": key, "delimiter": "/"}
            if after is not None:
                kwargs["after"] = after
            current = self._client.objects.list_objects(repo, ref, **kwargs)
            records += current.get("results")
            if not current.get("pagination").get("has_more"):
                break  # Done
            after = current.get("pagination").get("next_offset")

        # handle directories when not passing a trailing '/':
        if len(records) == 1:
            r = records[0]
            if r.get('path').endswith('/') and r.get('path_type') != 'object':
                return self._ls(repo, ref, key + '/')

        if detail:
            return [_object_stat_to_entry(repo, ref, f) for f in records]
        return [
            _remove_suffix(f'{repo}/{ref}/{d["path"]}', "/")
            if d.get("path_type") == "object"
            else f'{repo}/{ref}/{d["path"]}'
            for d in records
        ]

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        """Return raw bytes-mode file-like from the file-system"""
        return LakeFSBufferedFile(
            self,
            path,
            mode,
            block_size,
            autocommit,
            cache_options=cache_options,
            **kwargs,
        )

    def _rm(self, path):
        if isinstance(path, list):
            for file in path:
                self._rm(file)
            return
        repo, ref, key = _split_path(path)
        self._client.objects.delete_object(repo, ref, key)
        self.invalidate_cache(self._parent(path))

    def rm(self, path: Union[str, List[str]], recursive=False, maxdepth=None):
        if isinstance(path, list):
            for file in path:
                self.rm(file)
            return

        repo, ref, _ = _split_path(path)
        path_expand = self.expand_path(path, recursive=recursive, maxdepth=maxdepth)
        path_expand = [_split_path(file)[2] for file in path_expand]

        def chunks(lst: list, num: int):
            for i in range(0, len(lst), num):
                yield lst[i:i + num]

        for files in chunks(path_expand, 1000):
            self._client.objects.delete_objects(repo, ref, files)

        self.invalidate_cache(self._parent(path))

    def get_path(self, rpath, lpath, **kwargs):
        """
        Copy single remote path to local
        """
        if self.isdir(rpath):
            os.makedirs(lpath, exist_ok=True)
        else:
            self.get_file(rpath, lpath, **kwargs)

    def get_file(self, rpath, lpath, callback=None, **kwargs):
        if self.isdir(rpath):
            os.makedirs(lpath, exist_ok=True)
            return None
        repo, ref, key = _split_path(rpath)
        return self._client.objects.get_object(repo, ref, key)

    def put_file(self, lpath, rpath, callback=None, **kwargs):
        if os.path.isdir(lpath):
            self.makedirs(rpath, exist_ok=True)
        else:
            repo, ref, key = _split_path(rpath)
            with open(lpath, "rb") as out_file:
                self._client.objects.upload_object(repo, ref, key, content=out_file)
        self.invalidate_cache(self._parent(rpath))

    def created(self, path):
        """Return the created timestamp of a file as a datetime.datetime"""
        repo, ref, key = _split_path(path)
        if key:
            raise NotImplementedError("lakeFS objects have no created timestamp")
        if ref:
            raise NotImplementedError("lakeFS Refs have no created timestamp")
        repo_info = self._client.repositories.get_repository(repo)
        timestamp = repo_info.get("creation_date")
        return datetime.date.fromtimestamp(timestamp)

    def modified(self, path):
        return self.info(path).get("LastModified", None)

    def get_object(self, path: str, start: int, end: int) -> bytes:
        """
        Return object bytes in range
        """
        repo, ref, key = _split_path(path)
        try:
            stream = self._client.objects.get_object(repo, ref, key)
            data = stream.read()
        except Exception as e:
            raise ValueError(f'Error reading path: {path}: {e}')
        # TODO(remove this once a release that includes
        #  https://github.com/treeverse/lakeFS/pull/4623 is available on Pypi)
        if start is not None and end is not None:
            return data[start:end]
        if start is not None:
            return data[start:]
        if end is not None and end <= len(data):
            return data[:end]
        return data

    def isfile(self, path):
        repo, ref, key = _split_path(path)
        stat = self._client.objects.stat_object(repo, ref, key)
        return stat.get("path_type") == "object"

    def touch(self, path, truncate=True, **kwargs):
        if truncate or not self.exists(path):
            with self.open(path, "wb", **kwargs):
                pass
            self.invalidate_cache(self._parent(path))

    def pipe_file(self, path, value, **kwargs):
        """Set the bytes of given file"""
        repo, ref, key = _split_path(path)
        self._client.objects.upload_object(repo, ref, key)
        self.invalidate_cache(self._parent(path))

    def invalidate_cache(self, path=None):
        if path is None:
            self.dircache.clear()
        else:
            path = self._strip_protocol(path)
            path = path.lstrip("/")
            self.dircache.pop(path, None)
            while path:
                self.dircache.pop(path, None)
                path = self._parent(path)


class LakeFSBufferedFile(AbstractBufferedFile):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._tempfile = None

    def _upload_chunk(self, final=False):
        """Write one part of a multi-block file upload
        Parameters
        ==========
        final: bool
            This is the last block, so should complete file, if
            self.autocommit is True.
        """
        data = self.buffer.getvalue()
        if data:
            self._tempfile.write(data)

        if final:
            self.fs.put_file(self._tempfile.name, self.path)
            self._tempfile.close()

        return True

    def _initiate_upload(self):
        """Create remote file/upload"""
        self._tempfile = tempfile.NamedTemporaryFile("wb")
        self.loc = 0

    def _fetch_range(self, start, end):
        start = max(start, 0)
        end = min(self.size, end)
        if start >= end or start >= self.size:
            return b""
        return self.fs.get_object(self.path, start, end)


def register_fs(details):
    class ConfiguredLakeFSFilesystem(LakeFSNativeFS):
        def __init__(self, *args, **kwargs):
            super().__init__(
                key=details.access_key_id,
                secret=details.secret_access_key,
                host=details.endpoint_url,
                *args,
                **kwargs,
            )

    register_implementation("lakefs", ConfiguredLakeFSFilesystem)
