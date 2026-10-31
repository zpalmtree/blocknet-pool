import hashlib
import unittest
from audit_payout_balances import ALPHABET, address_problem


def encode(raw):
    number = int.from_bytes(raw, "big")
    result = ""
    while number:
        number, rem = divmod(number, 58)
        result = ALPHABET[rem] + result
    return "1" * (len(raw) - len(raw.lstrip(b"\0"))) + result


class AddressAuditTests(unittest.TestCase):
    def test_checksum_does_not_make_invalid_keys_payable(self):
        payload = b"\xff" * 64
        checksum = hashlib.sha3_256(b"blocknet_stealth_address_checksumblocknet_mainnet" + payload).digest()[:4]
        self.assertEqual(address_problem(encode(payload + checksum), lambda _: False), "invalid spend public key")
        self.assertIsNone(address_problem(encode(payload + checksum), lambda _: True))
        calls = iter([True, False])
        self.assertEqual(address_problem(encode(payload + checksum), lambda _: next(calls)), "invalid view public key")

    def test_rejects_wrong_network_and_malformed_addresses(self):
        self.assertEqual(address_problem("not-an-address", lambda _: True), "invalid base58")
        self.assertEqual(address_problem("111", lambda _: True), "unsupported address length")
        payload = b"\xff" * 64
        checksum = hashlib.sha3_256(b"blocknet_stealth_address_checksumblocknet_testnet" + payload).digest()[:4]
        self.assertEqual(address_problem(encode(payload + checksum), lambda _: True), "invalid mainnet checksum")


if __name__ == "__main__":
    unittest.main()
