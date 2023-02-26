import gzip
import json
import os
import os.path
import re
import sys
import time
from dataclasses import dataclass
from hashlib import md5
from io import BufferedIOBase, BytesIO
from typing import Dict, List, Optional, Set, Tuple, TypedDict
from uuid import uuid4

import boto3
import click


@click.group()
def cli():
    pass


@cli.command()
@click.option("--source", required=True, help="Directory to backup")
@click.option("--remote", required=True, help="S3 destination")
def backup(source: str, remote: str):
    cmd = Commands(ConcreteBackupIO(source, None, remote))
    cmd.backup()


@cli.command("list")
@click.option("--remote", required=True, help="S3 destination")
@click.option("--glob", help="Glob pattern")
def list_cmd(remote: str, glob: str):
    cmd = Commands(ConcreteBackupIO(None, None, remote))
    cmd.list_cmd(glob)


@cli.command()
@click.option("--remote", required=True, help="S3 destination")
@click.option("--destination", required=True, help="Directory to restore backup to")
@click.option("--glob", help="Glob pattern")
def restore(remote: str, destination: str, glob: str):
    cmd = Commands(ConcreteBackupIO(None, destination, remote))
    cmd.restore(glob)


INDEX_FILE_NAME = "snapshot.json.gz"


class RemoteFS:
    _base_path: str

    def __init__(self, base_path: str) -> None:
        self._base_path = base_path

    def read_file(self, path: str) -> Optional[bytes]:
        raise NotImplementedError()

    def read_file_into(self, path: str, file_obj: BufferedIOBase):
        raise NotImplementedError()

    def write_file(self, path: str, file_obj: BufferedIOBase):
        raise NotImplementedError()


class MemoryRemoteFS(RemoteFS):
    files: Dict[str, bytes]

    def __init__(self, base_path: str) -> None:
        super().__init__(base_path)

        self.files = {}

    def read_file(self, path: str) -> Optional[bytes]:
        path = os.path.join(self._base_path, path)
        return self.files.get(path, None)

    def read_file_into(self, path: str, file_obj: BufferedIOBase):
        file_obj.write(self.read_file(path))

    def write_file(self, path: str, file_obj: BufferedIOBase):
        self.files[os.path.join(self._base_path, path)] = file_obj.read()


class S3RemoteFS(RemoteFS):
    def __init__(self, base_path: str):
        bucket, base_key = base_path.split(os.sep, maxsplit=1)

        super().__init__(base_key)

        self._bucket = bucket
        self._s3 = boto3.client("s3")

    def read_file(self, path: str) -> Optional[bytes]:
        path = os.path.join(self._base_path, path)
        try:
            response = self._s3.get_object(Bucket=self._bucket, Key=path)
            return response["Body"].read()
        except self._s3.exceptions.NoSuchKey:
            return None

    def read_file_into(self, path: str, file_obj: BufferedIOBase):
        path = os.path.join(self._base_path, path)
        self._s3.download_fileobj(self._bucket, path, file_obj)

    def write_file(self, path: str, file_obj: BufferedIOBase):
        path = os.path.join(self._base_path, path)
        self._s3.upload_fileobj(file_obj, self._bucket, path)


class LocalFS:
    _base_dir: str

    def __init__(self, base_dir: str) -> None:
        self._base_dir = base_dir

    def list_files(self) -> List[str]:
        raise NotImplementedError()

    def open_file_read(self, path: str) -> BufferedIOBase:
        raise NotImplementedError()

    def open_file_write(self, path: str) -> BufferedIOBase:
        raise NotImplementedError()

    def mkdirp(self, path: Optional[str] = None) -> bool:
        raise NotImplementedError()


class OpenBytesIO(BytesIO):
    def close(self):
        pass


class MemoryLocalFS(LocalFS):
    dirs: Set[str]
    files: Dict[str, OpenBytesIO]

    def __init__(self, base_dir: str) -> None:
        super().__init__(base_dir)
        self.dirs = set()
        self.files = dict()

    def list_files(self) -> List[str]:
        # + 1 for /
        return [f[len(self._base_dir) + 1 :] for f in self.files.keys()]

    def open_file_read(self, path: str) -> BufferedIOBase:
        return OpenBytesIO(self.files[os.path.join(self._base_dir, path)].getvalue())

    def open_file_write(self, path: str) -> BufferedIOBase:
        path = os.path.join(self._base_dir, path)

        if os.path.dirname(path) not in self.dirs:
            raise Exception(f"Directory for {path} does not exist")

        self.files[path] = OpenBytesIO()

        return self.files[path]

    def mkdirp(self, path: Optional[str] = None) -> bool:
        path = os.path.join(self._base_dir, path) if path else self._base_dir
        if path in self.dirs:
            return False

        self.dirs.add(path)

        return True


class DiskLocalFS(LocalFS):
    def __init__(self, base_dir: str) -> None:
        super().__init__(base_dir)

    def list_files(self) -> List[str]:
        return [
            os.path.relpath(os.path.join(dir, file), self._base_dir)
            for dir, _, files in os.walk(self._base_dir)
            for file in files
        ]

    def open_file_read(self, path: str) -> BufferedIOBase:
        return open(os.path.join(self._base_dir, path), "rb")

    def open_file_write(self, path: str) -> BufferedIOBase:
        return open(os.path.join(self._base_dir, path), "xb")

    def mkdirp(self, path: Optional[str] = None) -> bool:
        path = os.path.join(self._base_dir, path) if path else self._base_dir
        try:
            os.makedirs(path, exist_ok=False)
            return True
        except FileExistsError:
            return False


class BackupIO:
    local_fs: LocalFS
    local_fs_out: LocalFS
    remote_fs: RemoteFS

    def print(self, msg: str):
        raise NotImplementedError()

    def print_err(self, msg: str):
        raise NotImplementedError()

    def exit(code: int):
        raise NotImplementedError()


class MemoryBackupIO(BackupIO):
    msgs: List[str]
    code: int

    def __init__(
        self, local_base_dir: str, local_out_base_dir: str, remote_base_dir: str
    ):
        self.msgs = []
        self.code = 0

        self.local_fs = MemoryLocalFS(local_base_dir)
        self.local_fs_out = MemoryLocalFS(local_out_base_dir)
        self.remote_fs = MemoryRemoteFS(remote_base_dir)

    def print(self, msg: str):
        self.msgs.append(msg)

    def print_err(self, msg: str):
        self.msgs.append(msg)

    def exit(self, code: int):
        self.code = code


class ConcreteBackupIO(BackupIO):
    def __init__(
        self, local_base_dir: str, local_out_base_dir: str, remote_base_dir: str
    ):
        self.local_fs = DiskLocalFS(local_base_dir)
        self.local_fs_out = DiskLocalFS(local_out_base_dir)
        self.remote_fs = S3RemoteFS(remote_base_dir)

    def print(self, msg: str):
        print(msg)

    def print_err(self, msg: str):
        print(msg, file=sys.stderr)

    def exit(self, code: int):
        sys.exit(code)


class Commands:
    _io: BackupIO

    def __init__(self, io: BackupIO):
        self._io = io

    def backup(self, flush_after: int = 50):
        assert flush_after > 0

        n = 0

        backup_index = BackupIndex(self._io.remote_fs)
        for diff_item in diff(self._io.local_fs, backup_index):
            file_id, should_copy = backup_index.add(diff_item.path, diff_item.md5)

            if should_copy:
                self._io.print(f'Copying {diff_item.path}...')
                with self._io.local_fs.open_file_read(diff_item.path) as file_obj:
                    self._io.remote_fs.write_file(f"obj/{file_id}", file_obj)

            n = (n + 1) % flush_after
            if n == 0:
                backup_index.flush()

        if n > 0:
            backup_index.flush()

    def list_cmd(self, glob: str):
        backup_index = BackupIndex(self._io.remote_fs)
        for file_snap in backup_index.list(glob or "*"):
            self._io.print(file_snap["path"])

    def restore(self, glob: str):
        if not self._io.local_fs_out.mkdirp():
            self._io.print_err("Destination already exists")
            self._io.exit(1)
            return

        backup_index = BackupIndex(self._io.remote_fs)
        for file_snap in backup_index.list(glob or "*"):
            dir = os.path.dirname(file_snap["path"])
            base = os.path.basename(file_snap["path"])

            out_file = os.path.join(dir, base)

            self._io.local_fs_out.mkdirp(dir)
            with self._io.local_fs_out.open_file_write(out_file) as out_obj:
                self._io.remote_fs.read_file_into(
                    f"obj/{file_snap['file_id']}", out_obj
                )


class SnapshotFile(TypedDict):
    file_id: str
    path: str
    md5: str


class IndexSnapshot(TypedDict):
    snapshot_id: str
    backup_time: float
    prev_snapshot_id: Optional[str]
    files: List[SnapshotFile]


class BackupIndex:
    _remote: RemoteFS
    _snapshots: List[IndexSnapshot]
    _new_snapshot: List[SnapshotFile]
    _last_snapshot_id: Optional[str]
    _index: Dict[str, SnapshotFile]
    _file_by_md5: Dict[str, SnapshotFile]

    def __init__(self, remote: RemoteFS):
        self._remote = remote
        self._snapshots = []
        self._new_snapshot = []
        self._last_snapshot_id = None
        self._index = None
        self._file_by_md5 = {}

    def _get_index(self):
        if self._index is not None:
            return self._index

        snapshot_content = self._remote.read_file(INDEX_FILE_NAME)
        if snapshot_content:
            self._snapshots = json.loads(gzip.decompress(snapshot_content))
        else:
            self._snapshots = []

        self._index = {}
        for snap in self._snapshots:
            assert self._last_snapshot_id == snap["prev_snapshot_id"]

            self._last_snapshot_id = snap["snapshot_id"]

            for snap_file in snap["files"]:
                self._index[snap_file["path"]] = snap_file
                self._file_by_md5[snap_file["md5"]] = snap_file

        return self._index

    def flush(self):
        self._snapshots.append(
            IndexSnapshot(
                snapshot_id=str(uuid4()),
                backup_time=time.time(),
                prev_snapshot_id=self._last_snapshot_id,
                files=self._new_snapshot,
            ),
        )
        self._last_snapshot_id = self._snapshots[-1]["snapshot_id"]
        self._new_snapshot = []

        out = BytesIO()
        out.write(gzip.compress(json.dumps(self._snapshots).encode("utf8")))
        out.seek(0)

        self._remote.write_file(INDEX_FILE_NAME, out)

    def list(self, glob: str) -> List[SnapshotFile]:
        parts = glob.split("*")
        pattern_parts = ["^"]
        for i, part in enumerate(parts):
            if i > 0:
                pattern_parts.append(".*")
            pattern_parts.append(re.escape(part))
        pattern_parts.append("$")

        pattern = re.compile("".join(pattern_parts))

        return [
            value
            for key, value in self._get_index().items()
            if pattern.match(key) and value["md5"] != ""
        ]

    def add(self, path: str, file_md5: str) -> Tuple[str, bool]:
        if file_md5 and (existing := self._file_by_md5.get(file_md5)):
            self._new_snapshot.append(
                SnapshotFile(
                    file_id=existing["file_id"], path=path, md5=existing["md5"]
                )
            )
            self._index[path] = self._new_snapshot[-1]
            self._file_by_md5[file_md5] = self._new_snapshot[-1]
            return existing["file_id"], False

        self._new_snapshot.append(
            SnapshotFile(file_id=str(uuid4()), path=path, md5=file_md5)
        )
        self._index[path] = self._new_snapshot[-1]
        self._file_by_md5[file_md5] = self._new_snapshot[-1]

        return self._new_snapshot[-1]["file_id"], file_md5 != ""


def file_md5(local: LocalFS, path: str) -> str:
    with local.open_file_read(path) as file_obj:
        hash = md5()

        pos = file_obj.tell()
        file_obj.seek(0)
        while data := file_obj.read(1024 * 1024):
            hash.update(data)
        file_obj.seek(pos)

        return hash.hexdigest()


@dataclass
class DiffItem:
    path: str
    md5: str


def should_backup(local_file: str) -> bool:
    parts = set(os.path.normpath(local_file).split(os.sep))
    to_exclude = set(
        [
            ".env",
            ".venv",
            "venv",
            "virtualenv",
            ".virtualenv",
            "__pycache__",
            ".mypy_cache",
            "node_modules",
            ".DS_Store",
        ]
    )

    return len(parts & to_exclude) == 0


def diff(local: LocalFS, backup_index: BackupIndex) -> List[DiffItem]:
    res: List[DiffItem] = []

    local_files = local.list_files()
    local_files = [
        local_file for local_file in local_files if should_backup(local_file)
    ]
    local_files_set = set(local_files)

    remote_files = backup_index.list("*")
    remote_files_map = {
        remote_file["path"]: remote_file for remote_file in remote_files
    }

    for remote in backup_index.list("*"):
        if remote["path"] not in local_files_set:
            res.append(DiffItem(path=remote["path"], md5=""))

    for local_file in local_files:
        item_md5 = file_md5(local, local_file)
        if (
            local_file not in remote_files_map
            or item_md5 != remote_files_map[local_file]["md5"]
        ):
            res.append(DiffItem(path=local_file, md5=item_md5))

    return res


if __name__ == "__main__":
    cli()
