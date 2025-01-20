from app.connection import * 

def data_scrapper():
    query = """SELECT DISTINCT ON (contenedor) *
FROM scrapper_retiros
ORDER BY contenedor"""
    df = connectionDB_todf(query)
    return df