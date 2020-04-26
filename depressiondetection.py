from pathlib import Path
from keras.models import Sequential
from keras.layers import Dense
from keras.utils import to_categorical
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
import matplotlib as plt
import argparse
import operator
import os
import sys
import numpy
import pandas as pd
import sys
from emotion_and_depression_detection.FacialExpression import storage


def main(args):
    args.storage = storage.Storage()
    df = pd.read_csv('alphabet.csv')
    dataset = df.values

    x = dataset[:,0:20]
    y = dataset[:,20]
    y = to_categorical(y)
    min_max_scaler = MinMaxScaler()
    x_scale=min_max_scaler.fit_transform(x)

    x_train, x_val_and_test, y_train, y_val_and_test= train_test_split(x_scale,y,test_size=0.2)
    x_val,x_test,y_val,y_test=train_test_split(x_val_and_test,y_val_and_test,test_size=0.1)

    model = Sequential()
    model.add(Dense(units=32,activation='relu',input_dim=20))
    model.add(Dense(units=32,activation='relu'))
    model.add(Dense(units=32,activation='relu'))
    model.add(Dense(units=4,activation='softmax'))

    model.compile(optimizer='adam',loss='categorical_crossentropy',metrics=['accuracy'])
    hist=model.fit(x_train,y_train,batch_size=10,epochs=100,validation_data=(x_val,y_val))
    a=model.evaluate(x_test,y_test)[1]
    input=args.storage.read_data_bucket(args.bucket_name,"user")
    for filename,inp in input:
        print(filename,inp)
    # b=model.predict(x_test)
    # input=[[0.2739726,0.43570669,0.33767873,0.17573222,0.28172043,0.23051189,
    # 0.744,0.82608696,0.76845039,0.21146245,0.18778281,0.17882888,0.45226131,0.39285714,0.4337518,0.12794547,0.30733674,0.27682529,0.2831906,0.26985475]]
    # args.storage.write_bucket(args,args.bucket_name,"user",input)
        result=numpy.asarray(inp)
        final_result=model.predict(result)
        for item in final_result:
            max_index, max_value = max(enumerate(item), key=operator.itemgetter(1))
            #print(max_index,max_value)
            depression_level=max_index+1
            print("depression level",depression_level)
        #print(final_result)
        os.remove(filename)
    # print(a)
    # print(b)

parser = argparse.ArgumentParser()
args = parser.parse_args()
args.python_path = os.environ.get('PYTHON_PATH', '')
args.google_key_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '')
args.param_connection_string = os.environ.get('MYSQL_CONNECTION_STRING', '')
args.environment = os.environ.get('ENVIRONMENT', '')
args.bucket_name = os.environ.get("BUCKET_NAME", 'user_data_depression')
sys.path.insert(0, args.python_path)
main(args)
