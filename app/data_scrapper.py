from app.connection import * 

def data_scrapper():
    query = """select * from scrapper_retiros"""
    df = connectionDB_todf(query)
    return df