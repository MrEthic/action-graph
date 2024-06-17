from operator import le
import typing as _t

from abc import ABC, abstractmethod
from pydantic import BaseModel, model_validator, Field
import asyncio
import logging
from datetime import datetime, timedelta

from pydantic._internal._generics import PydanticGenericMetadata

logger = logging.getLogger(__name__)


class CellNotExistError(KeyError): ...


class DuplicatedCellError(ValueError): ...


class BrainInterupt(Exception): ...


class BrainActivation(BaseModel):
    priority: int = Field(default=999, validation_alias="p")
    cell: str
    args: dict[str, _t.Any] = Field(default_factory=dict)

    def __getitem__(self, key: int) -> int:
        if key == 0:
            return self.priority
        else:
            raise IndexError(f"{self.__class__.__name__} have only one attribute to get (priority)")


class Cell(ABC, BaseModel):
    __instances: _t.ClassVar[list[str]] = []

    cell_type: _t.ClassVar[str]
    cell_name: str | None = None

    @model_validator(mode="after")
    def validate(self):
        if not self.cell_name:
            self.cell_name = self.cell_type + f"/{len(self.__class__.__instances)}"

    @abstractmethod
    def activate(self, *args, **kwargs) -> _t.Iterator[BrainActivation | str]:
        raise NotImplementedError()

    @classmethod
    def lastest(cls):
        return cls.__instances[-1]


class EndCell(Cell):
    cell_type = "end"
    cell_name: str = "end"

    def activate(self, *args, **kwargs) -> BrainActivation:
        raise BrainInterupt()


class Brain(_t.Mapping[str, Cell]):
    def __init__(
        self,
        *,
        timeout: int | timedelta | None = 60,
        start_signal: BrainActivation | None = None,
        strict: bool = False,
    ):
        if isinstance(timeout, int):
            timeout = timedelta(seconds=timeout)
        self._to = timeout
        self._cells = {}
        self._q = asyncio.PriorityQueue(maxsize=100)
        self._strict = strict

        self.add(EndCell())
        if start_signal:
            self._q.put_nowait(start_signal)
        else:
            self._warn_no_start_signal()

    def _warn_no_start_signal(self, do_raise: bool = False):
        logger.warning(
            f"{self.__class__.__name__} instantiated without 'start_signal'. "
            f"Use `.set_signal(..)` to set a start signal before starting the {self.__class__.__name__}."
        )
        if do_raise:
            raise RuntimeError("Missing start signal")

    def __contains__(self, key: Cell | str) -> bool:
        if isinstance(key, Cell):
            return key.cell_name in self._cells
        else:
            return key in self._cells

    def __getitem__(self, key: str) -> Cell:
        try:
            return self._cells[key]
        except KeyError as ke:
            if self._strict:
                raise CellNotExistError(*ke.args) from ke
            else:
                logger.warning(
                    f"{self.__class__.__name__} is not in strict mode. "
                    f"Unable to find cell {key} in {self.__class__.__name__}, trying with {key}/0"
                )
                return self.__getitem__(f"{key}/0")

    def __iter__(self) -> _t.Iterator[str]:
        return iter(self._cells)

    def __len__(self) -> int:
        return len(self._cells)

    def add(self, cell: Cell):
        if cell in self:
            raise DuplicatedCellError()
        self._cells[cell.cell_name] = cell

    async def emit(self, activation: BrainActivation):
        await self._q.put(activation)

    async def arun(self, start_signal: BrainActivation | None = None):
        if start_signal is not None:
            if start_signal.priority != 0:
                raise RuntimeError("Start signal should have priority of 0")
            await self.emit(start_signal)
        if self._q.empty():
            self._warn_no_start_signal(do_raise=True)
        empty_from: datetime | None = None
        while True:
            if self._to and self._q.empty():
                if empty_from and datetime.now() - empty_from > self._to:
                    logger.warning(f"Brain stopping as no signals detected in {self._to}")
                    break
                elif empty_from is None:
                    empty_from = datetime.now()
            elif self._q.empty():
                logger.debug("Brain waiting")
                await asyncio.sleep(0.5)
            else:
                try:
                    activation: BrainActivation = self._q.get_nowait()
                    logger.info(f"Running {activation.cell} with {activation.args}")
                    next_signals = self[activation.cell].activate(**activation.args)
                    for sig in next_signals:
                        if isinstance(sig, str):
                            sig = BrainActivation(cell=sig, priority=activation.priority)
                        await self.emit(sig)
                except BrainInterupt:
                    break
        logger.info("End")


class PrintCell(Cell):
    cell_type = "print"

    def activate(self, data):
        print(data)
        return ["end"]


b = Brain()
b.add(PrintCell())

logging.basicConfig(level=logging.INFO)

asyncio.run(b.arun(start_signal=BrainActivation(cell="print", p=0, args=dict(data="Boom"))))
