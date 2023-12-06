#!/usr/bin/env python3
from sqlalchemy import create_engine
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import pickle
from decouple import config
import logging
from logging.handlers import RotatingFileHandler
import Factory
import os


def PrepareData(data):
    logging.debug('Entering PrepareData function')

    lag_periods = 5
    for i in range(1, lag_periods + 1):
        data[f'lag_{i}'] = data['price'].shift(i)

    data = data.dropna()

    X = data.drop(['price', "date", "cryptocurrency_id", "id"], axis=1)
    y = data['price']

    logging.debug('Exiting PrepareData function')

    return X

def GetPredictionModel():
    logging.debug('Entering GetPredictionModel function')

    StrOldModelPath = config("strPathOfModels") + config("strPathOfLitecoinModel")
    Model = pickle.load(open(StrOldModelPath, 'rb'))

    logging.debug('Exiting GetPredictionModel function')

    return Model


def SaveModelPredictionDataToModelPredictionTable(strIdOftheCrypto:str,intBuySignal:str,intSellSignal:str, ftLastCryptoValue:float, ftPrediction: float, Conn , strQueryName:str="StrAddTodateToModelPredictionTableQuery"):
    """
    This function allows to save model run data to execution table

    param : strCryptoSymbol:str
            symbol of the crypto to be checked

    returns : int
               index of the crypto in the crypto table
    
    """
    logging.debug("Entering SaveModelTrainDataToRealModelRunTable function")

    logging.info(f"Building query to put date into model update database for crypto {strIdOftheCrypto}")
    strBuiltQueryToAdd:str = config(strQueryName).format(strIdOftheCrypto,intBuySignal, intSellSignal, ftLastCryptoValue, ftPrediction)
    
    logging.info(f"Sending data for crypto {strIdOftheCrypto} to the model update database")
    cursor = Conn.cursor()
    cursor.execute(strBuiltQueryToAdd)
    Conn.commit()
    cursor.close()

    logging.debug("Exiting SaveModelTrainDataToRealModelRunTable function")




def main():

    
    
    LOGFILENAME = os.path.basename(__file__).replace("py", "log")

    logger = logging.getLogger()

    fhFileHandler = RotatingFileHandler(os.path.join(config("DEBUGDIR"), LOGFILENAME),\
                                        maxBytes=10 * 1024 * 1024, backupCount=3)
    ffFormatter = logging.Formatter('%(asctime)s|%(module)s|%(lineno)d|%(levelname)s|%(message)s')
    fhFileHandler.setFormatter(ffFormatter)

    # Set the log level for the file handler
    fhFileHandler.setLevel(logging.DEBUG)
    # Set the log level for the logger
    logger.setLevel(logging.DEBUG)

    logger.addHandler(fhFileHandler)

    logging.info('create an engine to connect to the database')
    engine = Factory.GetConnectionEngineToDatabase()

    logging.info('get the data from the database for Litecoin')
    dfCryptoData = Factory.ReadDataFromDatabase(engine, strCryptoQuery="strQueryLitecoinTable")

    logging.info('Preparing data for prediction for Litecoin')
    X = PrepareData(dfCryptoData)

    logging.info('Loading prediction model for Litecoin')
    Prediction_Model = GetPredictionModel()

    logging.info('Perform prediction for Litecoin')
    prediction = Prediction_Model.predict(X)

    logging.info('Getting connection object from factory ConnectToDatabase function')
    Conn = Factory.ConnectToDatabase()

    logging.info('Check if the price of the crypto is going to rise ou la dernière valeur enregistré est {} et la prédiction est {}'.format(dfCryptoData["price"].iloc[-1], prediction[-1]))

    if prediction[-1] > dfCryptoData["price"].iloc[-1]:
        logging.info('Save Litecoin sell signal as the price is going to rise')
        SaveModelPredictionDataToModelPredictionTable(strIdOftheCrypto=3, intBuySignal=0, intSellSignal=1, ftLastCryptoValue = dfCryptoData["price"].iloc[-1], ftPrediction = prediction[-1],Conn=Conn)
    else:
        logging.info('Save Litecoin buy signal as the price is going to lower')
        SaveModelPredictionDataToModelPredictionTable(strIdOftheCrypto=3, intBuySignal=1, intSellSignal=0,ftLastCryptoValue = dfCryptoData["price"].iloc[-1], ftPrediction = prediction[-1], Conn=Conn)


if __name__ == "__main__":

    main()
