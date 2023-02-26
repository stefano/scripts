import gzip
from hashlib import md5
import json
import unittest
import os

from .__main__ import Commands, MemoryBackupIO, OpenBytesIO


class BackupTest(unittest.TestCase):
    def setUp(self):
        self.io = MemoryBackupIO("relative/root", "destination/dir", "r/w")
        self.cmds = Commands(self.io)

    def _set_files(self, files):
        self.io.local_fs.files = {
            os.path.join("relative/root", path): OpenBytesIO(content)
            for path, content in files.items()
        }

    def _get_file(self, files, path):
        filtered = [f for f in files if f["path"] == path]

        self.assertEqual(len(filtered), 1)

        return filtered[0]

    def _md5(self, content):
        hash = md5()
        hash.update(content)

        return hash.hexdigest()

    def _assert_index_linked(self, index0, index1, index_before=None):
        self.assertLess(index0["backup_time"], index1["backup_time"])

        self.assertTrue(index0["snapshot_id"])
        self.assertTrue(index1["snapshot_id"])
        self.assertNotEqual(index0["snapshot_id"], index1["snapshot_id"])
        if index_before:
            self.assertEqual(index0["prev_snapshot_id"], index_before["snapshot_id"])
        else:
            self.assertIsNone(index0["prev_snapshot_id"])
        self.assertEqual(index1["prev_snapshot_id"], index0["snapshot_id"])

    def _assert_remote_file(self, files, path, content):
        f1 = self._get_file(files, path)
        self.assertEqual(f1["md5"], self._md5(content))
        self.assertEqual(
            self.io.remote_fs.files.get(f"r/w/obj/{f1['file_id']}"), content
        )

    def test_backup(self):
        self._set_files(
            {
                "a/b/x/y.txt": b"abc",
                "a/b/x/z/u.txt": b"ghy",
                "a/b/a.txt": b"uuu",
                "a/b/u/p": b"",
            }
        )

        files = dict(**self.io.local_fs.files)

        self.cmds.backup()
        self.io.msgs = []

        self.assertEqual(self.io.local_fs.files, files)
        self.assertEqual(self.io.msgs, [])

        self.assertEqual(len(self.io.remote_fs.files), 5)

        indexes = json.loads(
            gzip.decompress(self.io.remote_fs.files["r/w/snapshot.json.gz"])
        )
        self.assertEqual(len(indexes), 1)

        index = indexes[0]
        files = index["files"]

        self.assertIsNone(index["prev_snapshot_id"])
        self.assertTrue(index["snapshot_id"])
        self.assertTrue(index["backup_time"])
        self.assertEqual(len(index["files"]), 4)

        f1 = self._get_file(files, "a/b/x/y.txt")
        self.assertEqual(f1["md5"], self._md5(b"abc"))
        self.assertEqual(
            self.io.remote_fs.files.get(f"r/w/obj/{f1['file_id']}"), b"abc"
        )

        f2 = self._get_file(files, "a/b/x/z/u.txt")
        self.assertEqual(f2["md5"], self._md5(b"ghy"))
        self.assertEqual(
            self.io.remote_fs.files.get(f"r/w/obj/{f2['file_id']}"), b"ghy"
        )

        f3 = self._get_file(files, "a/b/a.txt")
        self.assertEqual(f3["md5"], self._md5(b"uuu"))
        self.assertEqual(
            self.io.remote_fs.files.get(f"r/w/obj/{f3['file_id']}"), b"uuu"
        )

        f4 = self._get_file(files, "a/b/u/p")
        self.assertEqual(f4["md5"], self._md5(b""))
        self.assertEqual(self.io.remote_fs.files.get(f"r/w/obj/{f4['file_id']}"), b"")

    def test_backup_incremental_add(self):
        self._set_files(
            {
                "a/b/x/y.txt": b"abc",
                "a/b/u/p": b"",
            }
        )
        self.cmds.backup()
        self.io.local_fs.files["relative/root/a/b/a.txt"] = OpenBytesIO(b"uuu")

        self.cmds.backup()

        self.assertEqual(len(self.io.remote_fs.files), 4)

        indexes = json.loads(
            gzip.decompress(self.io.remote_fs.files["r/w/snapshot.json.gz"])
        )
        self.assertEqual(len(indexes), 2)
        index0, index1 = indexes
        files0 = index0["files"]
        files1 = index1["files"]

        self._assert_index_linked(index0, index1)

        self.assertEqual(len(files0), 2)
        self.assertEqual(len(files1), 1)

        f1 = self._get_file(files0, "a/b/x/y.txt")
        self.assertEqual(f1["md5"], self._md5(b"abc"))
        self.assertEqual(
            self.io.remote_fs.files.get(f"r/w/obj/{f1['file_id']}"), b"abc"
        )

        f2 = self._get_file(files0, "a/b/u/p")
        self.assertEqual(f2["md5"], self._md5(b""))
        self.assertEqual(self.io.remote_fs.files.get(f"r/w/obj/{f2['file_id']}"), b"")

        f3 = self._get_file(files1, "a/b/a.txt")
        self.assertEqual(f3["md5"], self._md5(b"uuu"))
        self.assertEqual(
            self.io.remote_fs.files.get(f"r/w/obj/{f3['file_id']}"), b"uuu"
        )

    def test_backup_incremental_remove(self):
        self._set_files(
            {
                "a/b/x/y.txt": b"abc",
                "a/b/u/p": b"",
            }
        )
        self.cmds.backup()
        del self.io.local_fs.files["relative/root/a/b/x/y.txt"]

        self.cmds.backup()

        self.assertEqual(len(self.io.remote_fs.files), 3)

        indexes = json.loads(
            gzip.decompress(self.io.remote_fs.files["r/w/snapshot.json.gz"])
        )
        self.assertEqual(len(indexes), 2)
        index0, index1 = indexes
        files0 = index0["files"]
        files1 = index1["files"]

        self._assert_index_linked(index0, index1)

        self.assertEqual(len(files0), 2)
        self.assertEqual(len(files1), 1)

        f1 = self._get_file(files0, "a/b/x/y.txt")
        self.assertEqual(f1["md5"], self._md5(b"abc"))
        self.assertEqual(
            self.io.remote_fs.files.get(f"r/w/obj/{f1['file_id']}"), b"abc"
        )

        f2 = self._get_file(files0, "a/b/u/p")
        self.assertEqual(f2["md5"], self._md5(b""))
        self.assertEqual(self.io.remote_fs.files.get(f"r/w/obj/{f2['file_id']}"), b"")

        f3 = self._get_file(files1, "a/b/x/y.txt")
        self.assertEqual(f3["md5"], "")
        self.assertNotEqual(f1["file_id"], f3["file_id"])
        self.assertNotIn(f"r/w/obj/{f3['file_id']}", self.io.remote_fs.files)

    def test_backup_incremental_change(self):
        self._set_files(
            {
                "a/b/x/y.txt": b"abc",
                "a/b/u/p": b"",
            }
        )
        self.cmds.backup()
        self.io.local_fs.files["relative/root/a/b/x/y.txt"] = OpenBytesIO(b"abc2")

        self.cmds.backup()

        self.assertEqual(len(self.io.remote_fs.files), 4)

        indexes = json.loads(
            gzip.decompress(self.io.remote_fs.files["r/w/snapshot.json.gz"])
        )
        self.assertEqual(len(indexes), 2)
        index0, index1 = indexes
        files0 = index0["files"]
        files1 = index1["files"]

        self._assert_index_linked(index0, index1)

        self.assertEqual(len(files0), 2)
        self.assertEqual(len(files1), 1)

        f1 = self._get_file(files0, "a/b/x/y.txt")
        self.assertEqual(f1["md5"], self._md5(b"abc"))
        self.assertEqual(
            self.io.remote_fs.files.get(f"r/w/obj/{f1['file_id']}"), b"abc"
        )

        f2 = self._get_file(files0, "a/b/u/p")
        self.assertEqual(f2["md5"], self._md5(b""))
        self.assertEqual(self.io.remote_fs.files.get(f"r/w/obj/{f2['file_id']}"), b"")

        f3 = self._get_file(files1, "a/b/x/y.txt")
        self.assertEqual(f3["md5"], self._md5(b"abc2"))
        self.assertNotEqual(f1["file_id"], f3["file_id"])
        self.assertEqual(
            self.io.remote_fs.files.get(f"r/w/obj/{f3['file_id']}"), b"abc2"
        )

    def test_backup_incremental_no_changes(self):
        self._set_files(
            {
                "a/b/x/y.txt": b"abc",
                "a/b/u/p": b"",
            }
        )
        self.cmds.backup()
        files_before = dict(**self.io.remote_fs.files)

        self.cmds.backup()

        self.assertEqual(files_before, self.io.remote_fs.files)

    def test_backup_dedup_one_shot(self):
        self._set_files(
            {
                "a/b/x/y.txt": b"abc",
                "a/b/x/z/u.txt": b"ghy",
                "a/b/a.txt": b"abc",
            }
        )
        files = dict(**self.io.local_fs.files)

        self.cmds.backup()

        self.assertEqual(len(self.io.remote_fs.files), 3)

        indexes = json.loads(
            gzip.decompress(self.io.remote_fs.files["r/w/snapshot.json.gz"])
        )
        self.assertEqual(len(indexes), 1)
        files = indexes[0]["files"]

        f1 = self._get_file(files, "a/b/x/y.txt")
        self.assertEqual(f1["md5"], self._md5(b"abc"))
        self.assertEqual(
            self.io.remote_fs.files.get(f"r/w/obj/{f1['file_id']}"), b"abc"
        )

        f2 = self._get_file(files, "a/b/x/z/u.txt")
        self.assertEqual(f2["md5"], self._md5(b"ghy"))
        self.assertEqual(
            self.io.remote_fs.files.get(f"r/w/obj/{f2['file_id']}"), b"ghy"
        )

        f3 = self._get_file(files, "a/b/a.txt")
        self.assertEqual(f3["md5"], f1["md5"])
        self.assertEqual(f3["file_id"], f1["file_id"])

    def test_backup_dedup_incremental(self):
        self._set_files(
            {
                "a/b/x/y.txt": b"abc",
                "a/b/u/p": b"",
            }
        )
        self.cmds.backup()

        self.io.local_fs.files["relative/root/a/b/u/p"] = OpenBytesIO(b"abc")
        self.cmds.backup()

        self.assertEqual(len(self.io.remote_fs.files), 3)

        indexes = json.loads(
            gzip.decompress(self.io.remote_fs.files["r/w/snapshot.json.gz"])
        )
        self.assertEqual(len(indexes), 2)
        index0, index1 = indexes
        files0 = index0["files"]
        files1 = index1["files"]

        self._assert_index_linked(index0, index1)

        self.assertEqual(len(files0), 2)
        self.assertEqual(len(files1), 1)

        f1 = self._get_file(files0, "a/b/x/y.txt")
        self.assertEqual(f1["md5"], self._md5(b"abc"))
        self.assertEqual(
            self.io.remote_fs.files.get(f"r/w/obj/{f1['file_id']}"), b"abc"
        )

        f2 = self._get_file(files0, "a/b/u/p")
        self.assertEqual(f2["md5"], self._md5(b""))
        self.assertEqual(self.io.remote_fs.files.get(f"r/w/obj/{f2['file_id']}"), b"")

        f3 = self._get_file(files1, "a/b/u/p")
        self.assertEqual(f3["md5"], f1["md5"])
        self.assertEqual(f1["file_id"], f3["file_id"])

    def test_backup_remove_dedup_file(self):
        self._set_files(
            {
                "a/b/x/y.txt": b"abc",
                "a/b/x/z/u.txt": b"ghy",
                "a/b/a.txt": b"abc",
            }
        )
        self.cmds.backup()

        del self.io.local_fs.files["relative/root/a/b/x/y.txt"]
        self.cmds.backup()

        self.assertEqual(len(self.io.remote_fs.files), 3)

        indexes = json.loads(
            gzip.decompress(self.io.remote_fs.files["r/w/snapshot.json.gz"])
        )
        self.assertEqual(len(indexes), 2)
        files0 = indexes[0]["files"]
        files1 = indexes[1]["files"]

        self.assertEqual(len(files0), 3)
        self.assertEqual(len(files1), 1)

        f1 = self._get_file(files1, "a/b/x/y.txt")
        self.assertEqual(f1["md5"], "")
        self.assertIsNone(self.io.remote_fs.files.get(f"r/w/obj/{f1['file_id']}"))

        f2 = self._get_file(files0, "a/b/x/z/u.txt")
        self.assertEqual(f2["md5"], self._md5(b"ghy"))
        self.assertEqual(
            self.io.remote_fs.files.get(f"r/w/obj/{f2['file_id']}"), b"ghy"
        )

        f3 = self._get_file(files0, "a/b/a.txt")
        self.assertEqual(f3["md5"], self._md5(b"abc"))
        self.assertEqual(
            self.io.remote_fs.files.get(f"r/w/obj/{f3['file_id']}"), b"abc"
        )

    def test_backup_partial_index_commit(self):
        self._set_files(
            {
                "a/b/x/y.txt": b"abc",
                "a/b/x/z/u.txt": b"ghy",
                "a/b/a.txt": b"abc",
                "a/b/b.txt": b"abc2",
                "a/b/c.txt": b"abc3",
            }
        )
        self.cmds.backup(flush_after=2)

        self.assertEqual(len(self.io.remote_fs.files), 5)

        indexes = json.loads(
            gzip.decompress(self.io.remote_fs.files["r/w/snapshot.json.gz"])
        )
        self.assertEqual(len(indexes), 3)
        files0 = indexes[0]["files"]
        files1 = indexes[1]["files"]
        files2 = indexes[2]["files"]

        self.assertEqual(len(files0), 2)
        self.assertEqual(len(files1), 2)
        self.assertEqual(len(files2), 1)

        self._assert_index_linked(indexes[0], indexes[1])
        self._assert_index_linked(indexes[1], indexes[2], index_before=indexes[0])

        self._assert_remote_file(files0, "a/b/x/y.txt", b"abc")
        self._assert_remote_file(files0, "a/b/x/z/u.txt", b"ghy")
        self._assert_remote_file(files1, "a/b/a.txt", b"abc")
        self._assert_remote_file(files1, "a/b/b.txt", b"abc2")
        self._assert_remote_file(files2, "a/b/c.txt", b"abc3")

    def test_backup_ignore_patterns(self):
        self._set_files(
            {
                "a/b/c/y.txt": b"a",
                "node_modules0/b/c/y.txt": b"b",
                "a/node_modules0/c/y.txt": b"c",
                "a/b/node_modules/b/c/y.txt": b"d",
                "a/node_modules/c/y.txt": b"e",
                "a/b/c/node_modules": b"f",
            }
        )
        self.cmds.backup()

        self.assertEqual(len(self.io.remote_fs.files), 4)

        indexes = json.loads(
            gzip.decompress(self.io.remote_fs.files["r/w/snapshot.json.gz"])
        )
        self.assertEqual(len(indexes), 1)
        files0 = indexes[0]["files"]

        self.assertEqual(len(files0), 3)

        self._assert_remote_file(files0, "a/b/c/y.txt", b"a")
        self._assert_remote_file(files0, "node_modules0/b/c/y.txt", b"b")
        self._assert_remote_file(files0, "a/node_modules0/c/y.txt", b"c")

    def test_list_backup_all(self):
        self._set_files(
            {
                "a/b/x/y.txt": b"abc",
                "a/b/x/z/u.txt": b"ghy",
                "a/b/a.txt": b"abc",
                "a/b/b.txt": b"abc2",
                "a/b/c.txt": b"abc3",
            }
        )
        self.cmds.backup()
        self.io.msgs = []

        self.cmds.list_cmd("*")

        self.assertEqual(
            self.io.msgs,
            ["a/b/x/y.txt", "a/b/x/z/u.txt", "a/b/a.txt", "a/b/b.txt", "a/b/c.txt"],
        )

    def test_list_backup_leaf(self):
        self._set_files(
            {
                "a/b/x/y.py": b"abc",
                "a/b/x/z/u.txt": b"ghy",
                "a/b/a.txt": b"abc",
                "a/b/b.txt": b"abc2",
                "a/b/c.py": b"abc3",
            }
        )
        self.cmds.backup()
        self.io.msgs = []

        self.cmds.list_cmd("*.py")

        self.assertEqual(self.io.msgs, ["a/b/x/y.py", "a/b/c.py"])

    def test_list_backup_parents(self):
        self._set_files(
            {
                "a/b/x/y.py": b"abc",
                "a/b/x/z/u.txt": b"ghy",
                "a/b/a.txt": b"abc",
                "a/c/b.txt": b"abc2",
                "a/c/c.py": b"abc3",
            }
        )
        self.cmds.backup()
        self.io.msgs = []

        self.cmds.list_cmd("a/b/*")

        self.assertEqual(self.io.msgs, ["a/b/x/y.py", "a/b/x/z/u.txt", "a/b/a.txt"])

    def test_restore_dest_exists(self):
        self._set_files(
            {
                "a/b/x/y.py": b"abc",
                "a/b/x/z/u.txt": b"ghy",
                "a/b/a.txt": b"abc",
                "a/c/b.txt": b"abc2",
                "c.py": b"abc3",
            }
        )
        self.cmds.backup()
        self.io.msgs = []

        self.io.local_fs_out.dirs = {"destination/dir"}

        self.cmds.restore("*")

        self.assertEqual(self.io.code, 1)
        self.assertEqual(self.io.msgs, ["Destination already exists"])
        self.assertEqual(self.io.local_fs_out.files, {})

    def test_restore_all(self):
        self._set_files(
            {
                "a/b/x/y.py": b"abc",
                "a/b/x/z/u.txt": b"ghy",
                "a/b/a.txt": b"abc",
                "a/c/b.txt": b"abc2",
                "c.py": b"abc3",
            }
        )
        self.cmds.backup()
        files_before = dict(**self.io.local_fs.files)

        self.cmds.restore("*")

        self.assertEqual(self.io.local_fs.files, files_before)

        out_files = {
            key: value.getvalue() for key, value in self.io.local_fs_out.files.items()
        }
        self.assertEqual(
            out_files,
            {
                "destination/dir/a/b/x/y.py": b"abc",
                "destination/dir/a/b/x/z/u.txt": b"ghy",
                "destination/dir/a/b/a.txt": b"abc",
                "destination/dir/a/c/b.txt": b"abc2",
                "destination/dir/c.py": b"abc3",
            },
        )

    def test_restore_partial(self):
        self._set_files(
            {
                "a/b/x/y.py": b"abc",
                "a/b/x/z/u.txt": b"ghy",
                "a/b/a.txt": b"abc",
                "a/c/b.txt": b"abc2",
                "c.py": b"abc3",
            }
        )
        self.cmds.backup()

        self.cmds.restore("*.py")

        out_files = {
            key: value.getvalue() for key, value in self.io.local_fs_out.files.items()
        }
        self.assertEqual(
            out_files,
            {
                "destination/dir/a/b/x/y.py": b"abc",
                "destination/dir/c.py": b"abc3",
            },
        )


if __name__ == "__main__":
    unittest.main()
