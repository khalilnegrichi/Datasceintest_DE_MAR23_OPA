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




def TrainPredictionModel(data):
    logging.debug('Entering TrainPredictionModel function')

    lag_periods = 5
    for i in range(1, lag_periods + 1):
        data[f'lag_{i}'] = data['price'].shift(i)

    data = data.dropna()

    X = data.drop(['price', "date", "cryptocurrency_id", "id"], axis=1)
    y = data['price']

    # Prépration des jeux de test et entrannement
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, shuffle=False)

    # entrainnement du model
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    logging.debug('Exiting TrainPredictionModel function')

    return (model, X_test, y_test)


def CheckIfNewModelIsBetterThanOldModel(newModel, X_test, y_test):
    logging.debug('Entering CheckIfNewModelIsBetterThanOldModel function')

    StrOldModelPath = config("strPathOfModels") + config("strPathOfEthereumModel")
    oldModel = pickle.load(open(StrOldModelPath, 'rb'))

    ftOldModelScore = oldModel.score(X_test, y_test)
    ftNewModelScore = newModel.score(X_test, y_test)

    logging.debug("Old Model score is {}".format(ftOldModelScore))
    logging.debug("New Model score is {}".format(ftNewModelScore))

    logging.debug('Exiting CheckIfNewModelIsBetterThanOldModel function')

    if ftNewModelScore > ftOldModelScore:
        return (True, ftOldModelScore , ftNewModelScore)
    else:
        return (False, ftOldModelScore , ftNewModelScore)
    

def SaveModelToPath(model):
    logging.debug('Entering SaveModelToPath function')

    StrNewModelPath = config("strPathOfModels") + config("strPathOfEthereumModel")
    pickle.dump(model, open(StrNewModelPath, 'wb'))

    logging.debug('Exiting SaveModelToPath function')


def SaveModelTrainDataToRealModelRunTable(strIdOftheCrypto:str,intModelRun:str,intModelUpdate:str, ftOldModelScore:float, ftNewModelScore:float, Conn , strQueryName:str="StrAddTodateToModelUpdateTableQuery"):
    """
    This function allows to save model run data to execution table

    param : strCryptoSymbol:str
            symbol of the crypto to be checked

    returns : int
               index of the crypto in the crypto table
    
    """
    logging.debug("Entering SaveModelTrainDataToRealModelRunTable function")

    logging.info(f"Building query to put date into model update database for crypto {strIdOftheCrypto}")
    strBuiltQueryToAdd:str = config(strQueryName).format(strIdOftheCrypto,intModelRun, intModelUpdate, ftOldModelScore, ftNewModelScore)
    
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

    logging.info('get the data from the database')
    dfCryptoData = Factory.ReadDataFromDatabase(engine, strCryptoQuery="strQueryEthereumTable")

    logging.info('train the new model on the new data')
    model, X_test, y_test = TrainPredictionModel(dfCryptoData)

    logging.info('check if the new model is better performing than the old Ethereum model')
    blNewModelBetter,ftOldModelScore, ftNewModelScore  = CheckIfNewModelIsBetterThanOldModel(model, X_test, y_test)

    # if the new model is better, save it to the models folder
    logging.info('Getting connection object from factory ConnectToDatabase function')
    Conn = Factory.ConnectToDatabase()

    if blNewModelBetter:
        logging.info('Saving the new model to the model folder')
        SaveModelToPath(model)
        logging.info('Add the record with updated model = 1 to the model Ethereum table')
        SaveModelTrainDataToRealModelRunTable(strIdOftheCrypto=98, intModelRun=1, intModelUpdate=1, ftOldModelScore = ftOldModelScore, ftNewModelScore = ftNewModelScore , Conn=Conn)
    else:
        logging.info('Add the record with updated model = 0 to the model Ethereum table')
        SaveModelTrainDataToRealModelRunTable(strIdOftheCrypto=98, intModelRun=1, intModelUpdate=0, ftOldModelScore = ftOldModelScore, ftNewModelScore = ftNewModelScore, Conn=Conn)


if __name__ == "__main__":


    main()
