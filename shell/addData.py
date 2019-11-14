#  add data into asset database
# ra_portfolio_pos ra_portfolio ra_portfolio_alloc ra_portfolio_nav ra_portfolio_asset ra_portfolio_nav

import numpy as np
import pandas as pd
from sqlalchemy import *
from sqlhelper.database import batch
from sqlhelper.tableToDataFrame import toSQL, toDf
from db.RaPortfolioPos import *
from db.FundInfos import *
from ipdb import set_trace

def addPos():

    # new data
    df = pd.read_excel('data.xlsx', sheet_name = '回测子基金比重')
    df.columns = ['ra_date','ra_fund_wind_id','ra_fund_name','ra_fund_ratio']
    
    df['ra_portfolio_id'] = 'PO.JHAX10'
    
    df['ra_fund_code'] = df.ra_fund_wind_id
    df.ra_fund_code = df.ra_fund_code.apply(lambda x: x[0:6])

    baseName = 'base'
    sql = toSQL(baseName)
    sql = sql.query(fund_infos.fi_globalid, fund_infos.fi_code)
    sql = sql.filter(fund_infos.fi_code.in_(list(df.ra_fund_code)))
    sql = sql.statement
    mofangId = toDf(baseName, sql)
    mofangId.set_index(['fi_code'],inplace = True)
    df['ra_fund_id'] = 0
    
    for i in range(len(df)):
        if df['ra_fund_code'][i] in mofangId.index:
            df['ra_fund_id'][i] = mofangId[mofangId.index == df['ra_fund_code'][i]]['fi_globalid']

    df['ra_pool_id'] = '11110100' 
    df['ra_fund_type'] = 11101
    df.drop('ra_fund_wind_id',axis = 1, inplace = True)
    df.drop('ra_fund_name', axis =1, inplace = True)
    df.set_index(['ra_portfolio_id','ra_date','ra_fund_code'], inplace = True)
    print(df)
    set_trace()

    # old data
    baseName = 'asset_allocation'
    sql = toSQL(baseName)
    sql = sql.query(ra_portfolio_pos.ra_portfolio_id, ra_portfolio_pos.ra_date, ra_portfolio_pos.ra_fund_code, ra_portfolio_pos.ra_pool_id, ra_portfolio_pos.ra_fund_type, ra_portfolio_pos.ra_fund_ratio)
    sql = sql.filter(ra_portfolio_pos.ra_portfolio_id == 'PO.JHTAX')
    sql = sql.statement
    dfBase = toDf(baseName, sql)
    dfBase.set_index(['ra_portfolio_id','ra_date','ra_fund_code'], inplace = True)

    batch(baseName, 'ra_portfolio_pos', df, dfBase, delete = False)

if __name__ == '__main__':
    addPos()
