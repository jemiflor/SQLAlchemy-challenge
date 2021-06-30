# -*- coding: utf-8 -*-
"""
Created on Mon Jun 28 01:08:25 2021

@author: jemif
"""

# import Flask & SqlAlchemy
from flask import Flask, url_for, jsonify, abort

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect, Interval, distinct
from sqlalchemy.sql.expression import text

import datetime
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

def route_has_input_parameters(rule):
    defaults = rule.defaults if rule.defaults is not None else ()
    arguments = rule.arguments if rule.arguments is not None else ()
    return len(defaults) >= len(arguments)


#################################################
# Flask Routes
#################################################

@app.route("/")
def home():
    print("Server received request for 'Home' page...")
    routes = []
    for rule in app.url_map.iter_rules():
        if "GET" in rule.methods and route_has_input_parameters(rule):
            url = url_for(rule.endpoint, **(rule.defaults or {}))
            routes.append((url, rule.endpoint))
    
    result = {
        "Api": "SQLAlchemy-Challenge API Home Page",
        "Routes": routes
        }
    
    return jsonify(result) 


@app.route("/api/v1.0/precipitation")
def precipitation():
    print("Server received request for 'precipitation' route...")
    
    session = Session(engine)
    
    results = session.query(Measurement.date, Measurement.prcp).all()
    
    session.close()
    
    precipitation_results = []
    for date, prcp in results:
        precipitation_dictionary = {}
        precipitation_dictionary["date"] = date
        precipitation_dictionary["precipitation"] = prcp
        precipitation_results.append(precipitation_dictionary)
        
    return jsonify(precipitation_results)
   

@app.route("/api/v1.0/stations")
def stations():
    print("Server received request for 'station' route...")
    
    session = Session(engine)
    
    results = session.query(Station.station,
                            Station.name,
                            Station.latitude,
                            Station.longitude,
                            Station.elevation).all()
    
    session.close()
    
    station_results = []
    for station, name, latitude, longitude, elevation in results:
        station_dictionary = {}
        station_dictionary["id"] = station
        station_dictionary["name"] = name
        station_dictionary["latitude"] = latitude
        station_dictionary["longitude"] = longitude
        station_dictionary["elevation"] = elevation
        station_results.append(station_dictionary)
    
    return jsonify(station_results)

@app.route("/api/v1.0/tobs")
def tobs():
    print("Server received request for 'tobs' route...")
    
    session = Session(engine)
    
    #Join and group stations with their measurements by station id and 
    #station name to get count of measurements for each station. Order the
    #grouped results by count of measurements from highest to lowest
    stations_with_measurement_counts = session.query(
        Station.name, 
        Station.station, 
        Measurement.tobs, 
        func.count(Measurement.id)).\
    join(Station, 
         Station.station == Measurement.station).\
    group_by(Station.name).\
    order_by(func.count(Measurement.id).desc())
    
    #Station with large measurement count is the most active station
    #get the id and name of the active station
    active_station_id = stations_with_measurement_counts.first().station
    active_station_name = stations_with_measurement_counts.first().name
    
    #get the most recent date on which the last measurement was taken
    #on the most active station
    active_station_most_recent_measurement_date = session.query(
        Measurement.station, 
        func.max(Measurement.date)).\
    filter(Measurement.station == active_station_id).\
    first()
        
    # get the date that is exactly one year behind the most recent date on 
    # which the last measurement was taken on the most active station    
    analysis_start_date = (
        datetime.strptime(
            active_station_most_recent_measurement_date[1], 
            '%Y-%m-%d') - relativedelta(years=1)).strftime('%Y-%m-%d')    
    
    print(analysis_start_date)
    #use the last one year start and end dates to get the temperature
    #observations of the most active station 
    active_station_Last_one_year_temperature_measurements = session.query(
        Measurement.date, 
        Measurement.tobs).\
    filter(
        Measurement.date >= analysis_start_date,
        Measurement.date <= active_station_most_recent_measurement_date[1],
        Measurement.station == active_station_id).\
    order_by(Measurement.date.desc()).all()
    
    session.close()
    
    temperature_observations = []
    for date, temperature in active_station_Last_one_year_temperature_measurements:
        temperature_dictionary = {}
        temperature_dictionary["date"] = date
        temperature_dictionary["temperature"] = temperature
        temperature_observations.append(temperature_dictionary)
    
    return jsonify(temperature_observations)

@app.route("/api/v1.0/", defaults={'start': None, 'end': None})
@app.route("/api/v1.0/<start>/", defaults={'end': None})
@app.route("/api/v1.0/<start>/<end>/")
def temperaturestats(start, end):
    print("Server received request for 'temperaturestats' route...")
    
    #has_end_date = false
    try:
        end = parse(end).strftime('%Y-%m-%d')
        has_end_date = True
    except:
        has_end_date = False
        
    try:
        start = parse(start).strftime('%Y-%m-%d')
        has_start_date = True
    except:
        has_start_date = False  
        
    if has_start_date == False:
        abort(400, 'Start date is required in date format (YYYY-dd-mm)')
      
    session = Session(engine)        
      
    if has_start_date == True and  has_end_date == True:
        
        measurements = session.query(Measurement.date, 
                                     func.min(Measurement.tobs),
                                     func.avg(Measurement.tobs),
                                     func.max(Measurement.tobs)).\
            filter(Measurement.date >= start,
                   Measurement.date <= end).\
                group_by(Measurement.date).all()      
                
    else:
        measurements = session.query(Measurement.date, 
                                     func.min(Measurement.tobs),
                                     func.avg(Measurement.tobs),
                                     func.max(Measurement.tobs)).\
            filter(Measurement.date >= start).\
                group_by(Measurement.date).all() 
    
    session.close()
    
    measurements_results = []
    for date, TMIN, TAVG, TMAX in measurements:
        measurements_dictionary = {}
        measurements_dictionary["date"] = date,
        measurements_dictionary["tmin"] = TMIN,
        measurements_dictionary["tavg"] = TAVG,
        measurements_dictionary["tmax"] = TMAX
        measurements_results.append(measurements_dictionary)
        
    return jsonify(measurements_results)


if __name__ == "__main__":
    app.run(debug=True)
