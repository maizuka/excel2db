import itertools
import logging

from openpyxl import load_workbook
from sqlalchemy import Column, String, Integer, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

bookname = 'book.xlsx'
sheetname = 'Sheet1'
skip_rows = 0
skip_cols = 0
databaseURL = 'sqlite:///:memory:'
# logging.basicConfig(level=logging.INFO)


def sheet2rows(bookname, sheetname, skip_rows, skip_cols):
    ws = load_workbook(filename=bookname, read_only=True)[sheetname]
    rows = itertools.islice(ws.rows, skip_rows, None)
    headers = [c.value for c in itertools.islice(next(rows), skip_cols, None)]
    datarows = (_generate_datadict(r, headers, skip_cols) for r in rows)
    return (headers, datarows)


def _generate_datadict(row, headers, skip_cols=0):
    datarow = itertools.islice(row, skip_cols, None)
    for i, cell in enumerate(datarow):
        yield {headers[i]: cell.value}


def create_data_class(headers, Base, tablename='exceldata'):
    """
    Returns a class which maps a string list to table columns.
    It has an integer `id` column and string `data_xxx` columns, where `xxx` is the header name.
    The attribute names for headers are the result of `str2hex` and unique for the header names.
    """
    class DataClass(Base):
        __tablename__ = tablename

        id = Column(Integer, primary_key=True)
        for h in headers:
            vars()[str2hex(h)] = Column(f'data_{h}', String)

        def __repr__(self):
            datastr = ', '.join(
                [f'data_{h}=\'{getattr(self, str2hex(h))}\'' for h in headers])
            return "<%s(id=%d, %s)>" % (self.__class__.__name__, (self.id or -1), datastr)

    return DataClass


def str2hex(string):
    return format(hash(string), 'x')


def add_datarows_to_db(datarows, session, DataClass):
    for datarow in datarows:
        datadict = {}
        for d in datarow:
            datadict.update(d)
        if all([v is None for v in datadict.values()]):
            continue
        _add_to_session_from_dict(datadict, session, DataClass)

    return


def _add_to_session_from_dict(dictionary, session, DataClass):
    datavals = {str2hex(k): v for k, v in dictionary.items()}
    row = DataClass(**datavals)
    session.add(row)
    return


headers, datarows = sheet2rows(bookname, sheetname, skip_rows, skip_cols)
Base = declarative_base()
ExcelRow = create_data_class(headers, Base)
engine = create_engine(databaseURL, echo=bool(
    logging.root.level <= logging.INFO))
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()
add_datarows_to_db(datarows, session, ExcelRow)
session.commit()
print(session.query(ExcelRow).all())
