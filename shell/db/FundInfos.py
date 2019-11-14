
# created by zhaoliyuan

# all .py files in shell/db stands for different tables in database

from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class fund_infos(Base):
    __tablename__ = 'fund_infos'

    fi_globalid = Column(Integer, primary_key = True)
    fi_code = Column(String)
    fi_wind_id = Column(String)
    fi_name = Column(String)
    fi_common_name = Column(String)

