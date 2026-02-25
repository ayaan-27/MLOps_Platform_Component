from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base

meta = MetaData()
Base = declarative_base(metadata=meta)


class Auto_repr:
    def __repr__(self):
        items = ("%s = %r" % (k, v) for k, v in self.__dict__.items())
        return "<%s: {%s}>" % (self.__class__.__name__, ", ".join(items))

    def to_dict(self):
        temp = self.__dict__
        t = [key for key in temp.keys() if key.startswith("_")]
        for key in t:
            temp.pop(key)

        return temp
