from flask import Flask, request, jsonify
import pandas as pd
import os
import json

# initialising a flask app

application = Flask(__name__)

app = application

@app.route('/', methods = ['GET'])
def home_page():
    return 'Welcome to solar energy production_Calgary Data'


@app.route('/weather/nameandyear', methods = ['GET'])
def get_output():

        
    df=pd.read_csv(os.path.join('artifacts','clean_data.csv'))

    df_grouped = df.groupby(['name','year']).sum(numeric_only=True)
    df_reset_index = df_grouped.groupby(['name','year']).first()

    name = request.args.get(key='name',type=str)
    year = request.args.get(key='year',type=int)
    
    index_value = (name,year)
    
    result = df_reset_index.loc[index_value, :]
    
    str_obj = result.to_dict()
    print(str_obj['kWh'])
    
    return jsonify(str_obj['kWh'])


    
if __name__ == '__main__':
    app.run(host='127.0.0.1', port = 5000)


        
       


        