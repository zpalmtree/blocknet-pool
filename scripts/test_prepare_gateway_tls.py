import grp
import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


spec = importlib.util.spec_from_file_location(
    "prepare_tls", Path(__file__).with_name("prepare_gateway_tls.py")
)
tls = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tls)


class CertificatePreparationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.dest = self.root / "staged"
        self.group = grp.getgrgid(os.getgid()).gr_name
        self.issue()

    def issue(self, name="bntpool.com"):
        subprocess.run(
            ["openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt",
             "ec_paramgen_curve:P-256", "-nodes", "-days", "2", "-subj", f"/CN={name}",
             "-addext", f"subjectAltName=DNS:{name}", "-keyout", str(self.source / "privkey.pem"),
             "-out", str(self.source / "fullchain.pem")],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

    def refresh(self):
        tls.refresh(self.source, self.dest, self.group)

    def pair(self):
        return [(self.dest / name).read_bytes() for name in ("fullchain.pem", "privkey.pem")]

    def test_stages_readable_private_pair_and_is_idempotent(self):
        self.refresh()
        before = [(self.dest / name).stat().st_ino for name in ("fullchain.pem", "privkey.pem")]
        self.refresh()
        self.assertEqual(before, [(self.dest / name).stat().st_ino for name in ("fullchain.pem", "privkey.pem")])
        for name in ("fullchain.pem", "privkey.pem"):
            self.assertEqual((self.dest / name).stat().st_mode & 0o777, 0o640)
            self.assertEqual((self.dest / name).stat().st_gid, os.getgid())
        self.assertEqual(self.dest.stat().st_mode & 0o777, 0o750)

    def test_renewal_replaces_both_files(self):
        self.refresh()
        old = self.pair()
        self.issue()
        self.refresh()
        self.assertTrue(all(a != b for a, b in zip(old, self.pair())))

    def test_mismatched_pair_keeps_previous_certificate(self):
        self.refresh()
        old = self.pair()
        self.issue()
        (self.source / "privkey.pem").write_bytes(old[1])
        with self.assertRaisesRegex(ValueError, "do not match"):
            self.refresh()
        self.assertEqual(self.pair(), old)

    def test_wrong_hostname_keeps_previous_certificate(self):
        self.refresh()
        old = self.pair()
        self.issue("wrong.example")
        with self.assertRaisesRegex(ValueError, "does not cover"):
            self.refresh()
        self.assertEqual(self.pair(), old)

    def test_malformed_certificate_keeps_previous_certificate(self):
        self.refresh()
        old = self.pair()
        (self.source / "fullchain.pem").write_text("invalid certificate")
        with self.assertRaises(subprocess.CalledProcessError):
            self.refresh()
        self.assertEqual(self.pair(), old)

    def test_expired_certificate_keeps_previous_certificate(self):
        self.refresh()
        old = self.pair()
        expired = tls.openssl(
            "x509", "-in", self.source / "fullchain.pem", "-signkey",
            self.source / "privkey.pem", "-days", "-1",
        )
        (self.source / "fullchain.pem").write_bytes(expired)
        with self.assertRaises(subprocess.CalledProcessError):
            self.refresh()
        self.assertEqual(self.pair(), old)


if __name__ == "__main__":
    unittest.main()
