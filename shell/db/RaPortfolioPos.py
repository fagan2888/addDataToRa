
# created by zhaoliyuan

# all .py files in shell/db stands for different tables in database

from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ra_portfolio_pos(Base):
    __tablename__ = 'ra_portfolio_pos'

    ra_portfolio_id = Column(String, primary_key = True)
    ra_date = Column(Date, primary_key = True)
    ra_pool_id = Column(String, primary_key = True)
    ra_fund_id = Column(Integer, primary_key = True)
    ra_fund_code = Column(String)
    ra_fund_type = Column(Integer)
    ra_fund_ratio = Column(Float)
    
    created_at = Column(Time)
    updated_at = Column(Time)

