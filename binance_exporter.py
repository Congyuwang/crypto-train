from dataclasses import dataclass
import json
import logging
import tarfile
from pathlib import Path
from typing import IO
import argparse
import numpy as np
from datetime import datetime, timezone
from sortedcontainers.sorteddict import SortedDict
from binance import BinanceOrderBook, Snapshot, PartialUpdate, UpdateObj


LOGGER = logging.getLogger(__name__)
LOG_FORMAT = "%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s"
FILE_TS_FORMAT = "%Y%m%d_%H%M%S"
BTC_USDT_DEPTH = "btcusdt@depth@100ms"
BTC_USDT_SNAPSHOT = "binance.BTCUSDT@orderbook5000"


@dataclass
class ExportData:
    bids10: list[list[float]]
    asks10: list[list[float]]
    bids10_size: list[list[float]]
    asks10_size: list[list[float]]

    def __init__(self):
        self.bids10 = list()
        self.asks10 = list()
        self.bids10_size = list()
        self.asks10_size = list()

    def __len__(self):
        return len(self.bids10)

    def on_orderbook_update(self, orderbook: BinanceOrderBook):
        """Callback for orderbook updates."""
        best_bids10 = list(orderbook.best_bids(10))
        best_asks10 = list(orderbook.best_asks(10))
        self.bids10.append([b.price for b in best_bids10])
        self.bids10_size.append([b.quantity for b in best_bids10])
        self.asks10.append([a.price for a in best_asks10])
        self.asks10_size.append([a.quantity for a in best_asks10])

    def save_as_np(self, path: str):
        dir_path = Path(path).parent
        dir_path.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(
            path,
            bids10=np.array(self.bids10, dtype=np.float32),
            asks10=np.array(self.asks10, dtype=np.float32),
            bids10_size=np.array(self.bids10_size, dtype=np.float32),
            asks10_size=np.array(self.asks10_size, dtype=np.float32),
        )


def process_data_tar_gz(tar_path: str, out_dir: str):
    with tarfile.open(tar_path, "r:gz") as tar:
        orderbooks = build_orderbooks(tar, BTC_USDT_SNAPSHOT)
        export_data: dict[int, ExportData] = dict()
        for update in iter_updates(tar, BTC_USDT_DEPTH):
            apply_update_to_orderbooks(export_data, orderbooks, update)
        for id, data in export_data.items():
            ts = datetime.fromtimestamp(id, tz=timezone.utc)
            data.save_as_np(f"{out_dir}/{ts.strftime(FILE_TS_FORMAT)}.npz")


def apply_update_to_orderbooks(
    export_data: dict[int, ExportData],
    orderbooks: SortedDict[int, BinanceOrderBook],
    update: PartialUpdate,
):
    """Apply updates to orderbooks."""
    orderbooks_to_delete: list[int] = []
    for book_id, orderbook in orderbooks.items():
        try:
            if orderbook.update(update):
                on_orderbook_update(export_data, book_id, orderbook)
        except RuntimeWarning as e:
            LOGGER.warning(f"Failed to update orderbook-{book_id}: {e}.")
            orderbooks_to_delete.append(book_id)
    for book_id in orderbooks_to_delete:
        del orderbooks[book_id]


def on_orderbook_update(
    export_data: dict[int, ExportData],
    book_id: int,
    orderbook: BinanceOrderBook,
):
    """Callback for orderbook updates."""
    if book_id not in export_data:
        export_data[book_id] = ExportData()
    export_data[book_id].on_orderbook_update(orderbook)
    history_size = len(export_data[book_id])
    if history_size % 1000 == 0:
        LOGGER.info(f"Processed {history_size} orderbook updates for {book_id}.")


def build_orderbooks(
    tar: tarfile.TarFile, snapshot_prefix: str
) -> SortedDict[int, BinanceOrderBook]:
    """Build a dictionary of orderbooks from timestamps."""
    full_prefix = f"./{snapshot_prefix}"
    orderbooks: SortedDict[int, BinanceOrderBook] = SortedDict()
    for member in tar.getmembers():
        if member.name.startswith(full_prefix):
            timestamp = int(member.name.lstrip(f"{full_prefix}."))
            io = tar.extractfile(member)
            if io is None:
                raise ValueError(f"Failed to extract file {member.name}")
            snapshot = extract_snapshot(io)
            orderbooks[timestamp] = BinanceOrderBook(snapshot)
    return orderbooks


def extract_snapshot(io: IO[bytes]) -> Snapshot:
    """Extract a single snapshot."""
    text = io.read().decode("utf-8")
    _, json_data = text.split("|", 1)
    snapshot_obj: UpdateObj = json.loads(json_data)
    return Snapshot.from_object(snapshot_obj)


def iter_updates(tar: tarfile.TarFile, depth: str):
    """Iterate over partial updates."""
    depth_member = tar.getmember(f"./{depth}")
    io = tar.extractfile(depth_member)
    if io is None:
        raise ValueError(f"Failed to extract file {depth}")
    for line_byte in io:
        line = line_byte.decode("utf-8")
        _, json_data = line.split("|", 1)
        update_obj: UpdateObj = json.loads(json_data)["data"]
        assert update_obj["e"] == "depthUpdate"
        partial_update = PartialUpdate.from_object(update_obj)
        yield partial_update


@dataclass
class Args:
    tar_file_path: str
    out_dir: str


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    arg_parser = argparse.ArgumentParser()
    _ = arg_parser.add_argument("--tar_file_path", help="Path to the tar.gz file")
    _ = arg_parser.add_argument("--out_dir", help="Output directory")
    args = arg_parser.parse_args(namespace=Args)
    process_data_tar_gz(args.tar_file_path, args.out_dir)
