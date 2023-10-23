#!/usr/bin/env python3

import requests
import psycopg2
from decouple import config # permet d'uiliser le fichier .env
import logging
from datetime import datetime
from dateutil import parser

import Factory

# Coin API credentials
API_KEY = config("BTC_API_KEY")



# Insert cryptocurrency data into the database
def InsertData(Conn, ltCryptocurrencies):
    """
    This function allows to insert the connection object to the database 

    params:
        conn: 
            connection object to the database
        ltCryptocurrencies:
            list of cryptocurrencies to insert into the database


    """
    logging.debug('Entering InsertData function')

    cursor = Conn.cursor()
    cursor.executemany("INSERT INTO cryptocurrency (name, symbol, creation_date) VALUES (%s, %s, %s);", ltCryptocurrencies)
    
    Conn.commit()
    cursor.close()

    logging.debug('Existing InsertData function')

# Main function
def main():

    logging.debug('Entering Main function')


    # Connect to the database
    Conn = Factory.ConnectToDatabase()

    # Retrieve cryptocurrency information from Coin API
    StrURL = config("URLAssestListCryptos").format(API_KEY)
    response = requests.get(StrURL)


    if response.status_code == 200:
        logging.debug('Response received from the coin API endpoint')
        jnData = response.json()
        ltCryptocurrencies = []

        logging.info('Looping through the response {} elements'.format(len(jnData)))
        
        for item in jnData:
            if item['type_is_crypto'] == 1 and 'data_trade_start' in item :
                strName = item['name']
                strSymbol = item['asset_id']
                strCreationDate = parser.parse(item['data_trade_start']).strftime('%Y-%m-%d')   
                ltCryptocurrencies.append((strName, strSymbol, strCreationDate))

            else: 
                logging.debug('Asset {} is not a cryoptocurrency, it will not be added to the database'.format(item['name']))

    else:
        logging.error('Unable to connect to Coin API endpoint: response code recieved is {}'.format(response.status_code))

    #print(ltCryptocurrencies)
    #GetMaxCryoptLenght(ltCryptocurrencies)

    # Insert the cryptocurrency data into the database
    InsertData(Conn, ltCryptocurrencies)

    # Close the database connection
    Conn.close()

    logging.debug('Exiting Main function')


def GetMaxCryoptLenght(ltCryptocurrencies):

    for crypto in ltCryptocurrencies:
        if len(crypto[1]) > 15:
            print("The crypto Symbol {} has a length of {}".format(crypto[0], len(crypto[0])))



if __name__ == "__main__":
    
    print(config("API_KEY"))
    
    main()