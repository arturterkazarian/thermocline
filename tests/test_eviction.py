"""Unit tests for hot-tier eviction policies."""

from thermocline import LruEviction


class TestLruEviction:
    def test_victim_is_least_recently_admitted(self) -> None:
        lru: LruEviction[int] = LruEviction()
        lru.on_admit(1)
        lru.on_admit(2)
        lru.on_admit(3)
        assert lru.pick_victim() == 1

    def test_access_refreshes_recency(self) -> None:
        lru: LruEviction[int] = LruEviction()
        lru.on_admit(1)
        lru.on_admit(2)
        lru.on_access(1)
        assert lru.pick_victim() == 2

    def test_forget_removes_key(self) -> None:
        lru: LruEviction[int] = LruEviction()
        lru.on_admit(1)
        lru.on_admit(2)
        lru.forget(1)
        assert lru.pick_victim() == 2

    def test_forget_unknown_key_is_noop(self) -> None:
        lru: LruEviction[int] = LruEviction()
        lru.forget(42)
        lru.on_admit(1)
        assert lru.pick_victim() == 1

    def test_access_of_unknown_key_is_noop(self) -> None:
        lru: LruEviction[int] = LruEviction()
        lru.on_access(42)
        lru.on_admit(1)
        assert lru.pick_victim() == 1

    def test_readmit_refreshes_recency(self) -> None:
        lru: LruEviction[int] = LruEviction()
        lru.on_admit(1)
        lru.on_admit(2)
        lru.on_admit(1)
        assert lru.pick_victim() == 2
