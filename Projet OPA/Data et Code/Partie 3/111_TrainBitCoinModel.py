#!/usr/bin/env python3
from sqlalchemy import create_engine
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import pickle
from decouple import config
import logging

import Factory





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
        return True
    else:
        return False
    

def SaveModelToPath(model):
    logging.debug('Entering SaveModelToPath function')

    StrNewModelPath = config("strPathOfModels") + config("strPathOfBitcoinModel")
    pickle.dump(model, open(StrNewModelPath, 'wb'))

    logging.debug('Exiting SaveModelToPath function')


def SaveModelTrainDataToRealModelRunTable(strIdOftheCrypto:str,intModelRun:str,intModelUpdate:str, Conn , strQueryName:str="StrAddTodateToModelUpdateTableQuery"):
    """
    This function allows to save model run data to execution table

    param : strCryptoSymbol:str
            symbol of the crypto to be checked

    returns : int
               index of the crypto in the crypto table
    
    """
    logging.debug("Entering SaveModelTrainDataToRealModelRunTable function")

    logging.info(f"Building query to put date into model update database for crypto {strIdOftheCrypto}")
    strBuiltQueryToAdd:str = config(strQueryName).format(strIdOftheCrypto,intModelRun, intModelUpdate)
    
    logging.info(f"Sending data for crypto {strIdOftheCrypto} to the model update database")
    cursor = Conn.cursor()
    cursor.execute(strBuiltQueryToAdd)
    Conn.commit()
    cursor.close()

    logging.debug("Exiting SaveModelTrainDataToRealModelRunTable function")



def main():

    logging.info('create an engine to connect to the database')
    engine = Factory.GetConnectionEngineToDatabase()

    logging.info('get the data from the database')
    dfCryptoData = Factory.ReadDataFromDatabase(engine, strCryptoQuery="strQueryBitCoinTable")

    logging.info('train the new model on the new data')
    model, X_test, y_test = TrainPredictionModel(dfCryptoData)

    logging.info('check if the new model is better performing than the old model')
    blNewModelBetter = CheckIfNewModelIsBetterThanOldModel(model, X_test, y_test)

    # if the new model is better, save it to the models folder
    logging.info('Getting connection object from factory ConnectToDatabase function')
    Conn = Factory.ConnectToDatabase()

    if blNewModelBetter:
        logging.info('Saving the new model to the model folder')
        SaveModelToPath(model)
        logging.info('Add the record with updated model = 1 to the model table')
        SaveModelTrainDataToRealModelRunTable(strIdOftheCrypto=1,intModelRun=1,intModelUpdate=1, Conn=Conn)
    else:
        logging.info('Add the record with updated model = 0 to the model table')
        SaveModelTrainDataToRealModelRunTable(strIdOftheCrypto=1,intModelRun=1,intModelUpdate=0, Conn=Conn)


if __name__ == "__main__":
    main()
