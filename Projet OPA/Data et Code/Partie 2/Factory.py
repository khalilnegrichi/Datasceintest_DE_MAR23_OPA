#!/usr/bin/env python3

import psycopg2
import logging
from decouple import config



# Connect to the TimescaleDB database
def ConnectToDatabase():
    """
    This function creates a connection object to the database 
    The main paramters of the function are retrived from decouple config file
    """

    logging.debug('Entering ConnectToDatabase function')

    Conn = psycopg2.connect(
        host=config("host"),
        port=config("port"),
        database=config("database"),
        user=config("user"),
        password=config("password")
    )

    logging.debug('Existing ConnectToDatabase function')
    return Conn

def GetDataFromCryptocurrencyDatabase(Conn):
    """
    This function allow to fetch data from Cryptocurrency table and return it back for 
    
    params:
        conn: 
            connection object to the database

    """
    logging.debug('Entering GetDataFromCryptocurrencyDatabase function')


    logging.info('Getting the Cryptocurrency select SQL request query')
    strCryptoSelectRequestKey = config("StrCryptocurrencySelectRequest")

    logging.info('Fetching data from cryotocurrency table')
    cursor = Conn.cursor()
    cursor.execute(strCryptoSelectRequestKey)
    ltFetchedData = cursor.fetchall()
    cursor.close()

    logging.debug('Existing GetDataFromCryptocurrencyDatabase function')
    
    return ltFetchedData


def CheckIfCryptoSymbolIsAmongListToBeUpdated(strCryptoSymbol:str , strListKey:str="ltListOfSymbolToBeUpated" ) :
    """
    This function allows to check if the symbol of the crypto is among the list to be updated

    param : strCryptoSymbol:str
            symbol of the crypto to be checked



    returns : bool
                True if the symbol is among the list of acrynym to be updated
                False if the symbol is not among the list of acrynym to be updated
    
    """

    logging.debug('Entering CheckIfCryptoSymbolIsAmongListToBeUpdated function')

    ltListOfSymbolToBeUpdated:list = config(strListKey)

    if strCryptoSymbol in ltListOfSymbolToBeUpdated:
        logging.debug(f"The crypto symbol {strCryptoSymbol} is among the list of crypto to be updated")
        logging.debug('Exiting CheckIfCryptoSymbolIsAmongListToBeUpdated function')
        return True
    else:
        logging.debug(f"The crypto symbol {strCryptoSymbol} is not among the list of crypto to be updated")
        logging.debug('Exiting CheckIfCryptoSymbolIsAmongListToBeUpdated function')
        return False