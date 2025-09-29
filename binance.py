from dataclasses import dataclass
from collections.abc import Iterator
from sortedcontainers.sorteddict import SortedDict


PriceLevelObj = dict[str, float] | list[float]
UpdateObj = dict[str, list[PriceLevelObj] | int]


@dataclass
class PriceLevel:
    price: float
    quantity: float

    @staticmethod
    def from_object(obj: PriceLevelObj):
        if type(obj) is dict:
            return PriceLevel(price=float(obj["price"]), quantity=float(obj["qty"]))
        if type(obj) is list:
            return PriceLevel(price=float(obj[0]), quantity=float(obj[1]))
        raise TypeError(f"Expected dict, got {type(obj)}")


class Levels:
    def __init__(self):
        self.levels: SortedDict[float, PriceLevel] = SortedDict()

    def merge(self, levels: list[PriceLevel]):
        for level in levels:
            if level.quantity == 0.0:
                _ = self.levels.pop(level.price, None)
            else:
                self.levels[level.price] = level

    def max(self, i: int = 1) -> PriceLevel:
        return self.levels.peekitem(-i)[1]

    def max_slice(self, i: int, reverse: bool = False) -> Iterator[PriceLevel]:
        return (self.levels[p] for p in self.levels.islice(-i, None, reverse))

    def min(self, i: int = 1) -> PriceLevel:
        return self.levels.peekitem(i - 1)[1]

    def min_slice(self, i: int, reverse: bool = False) -> Iterator[PriceLevel]:
        return (self.levels[p] for p in self.levels.islice(None, i, reverse))

    def __len__(self):
        return len(self.levels)


@dataclass
class PartialUpdate:
    bids: list[PriceLevel]
    asks: list[PriceLevel]
    first_update_id: int
    last_update_id: int

    @staticmethod
    def from_object(obj: UpdateObj):
        bids = obj["b"]
        asks = obj["a"]
        first_update_id = obj["U"]
        last_update_id = obj["u"]
        assert type(bids) is list
        assert type(asks) is list
        assert type(first_update_id) is int
        assert type(last_update_id) is int
        return PartialUpdate(
            bids=[PriceLevel.from_object(o) for o in bids],
            asks=[PriceLevel.from_object(o) for o in asks],
            first_update_id=first_update_id,
            last_update_id=last_update_id,
        )


@dataclass
class Snapshot:
    bids: list[PriceLevel]
    asks: list[PriceLevel]
    last_update_id: int

    @staticmethod
    def from_object(obj: UpdateObj):
        bids = obj["bids"]
        asks = obj["asks"]
        last_update_id = obj["lastUpdateId"]
        assert type(bids) is list
        assert type(asks) is list
        assert type(last_update_id) is int
        return Snapshot(
            bids=[PriceLevel.from_object(o) for o in bids],
            asks=[PriceLevel.from_object(o) for o in asks],
            last_update_id=last_update_id,
        )


class BinanceOrderBook:
    def __init__(self, snapshot: Snapshot):
        self.bids: Levels = Levels()
        self.asks: Levels = Levels()
        self.bids.merge(snapshot.bids)
        self.asks.merge(snapshot.asks)
        self.last_update_id: int = snapshot.last_update_id

    def update(self, update: PartialUpdate) -> bool:
        """Return True on update, False on skip, and raise RuntimeWarning on invalid id."""
        if update.last_update_id < self.last_update_id:
            return False
        if update.first_update_id > (self.last_update_id + 1):
            raise RuntimeWarning(
                f"Received update with id {update.first_update_id} but expected {self.last_update_id + 1}"
            )
        self.bids.merge(update.bids)
        self.asks.merge(update.asks)
        self.last_update_id = update.last_update_id
        return True

    def best_bid(self, i: int = 1):
        return self.bids.max(i)

    def best_bids(self, n: int) -> Iterator[PriceLevel]:
        return self.bids.max_slice(n, reverse=True)

    def best_ask(self, i: int = 1):
        return self.asks.min(i)

    def best_asks(self, n: int) -> Iterator[PriceLevel]:
        return self.asks.min_slice(n)

    def __len__(self):
        return len(self.bids) + len(self.asks)
