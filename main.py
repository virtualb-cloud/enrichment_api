
from flask import Flask, request, jsonify
from threading import Thread
import os
import json

# samples
from modules.read_samples import Read
from modules.read_metrics import Read_metrics
from background_tasks import inserter, trainer, deleter
from modules.insert_controller import Insert_controller
from modules.train_controller import Train_controller
from modules.delete_controller import Delete_controller

# predicts
from modules.predict_models import Predict
from modules.predict_controller import Predict_controller



# Config
app = Flask(__name__)
app.config['ENV'] = 'development'

@app.route("/predict_needs", methods=["POST"])
def predict_needs():
    
    if request.method == "POST":

        controller = Predict_controller()
        predict = Predict(purpose="Needs")
        
        body = request.get_json(force=True, silent=False, cache=True)

        # call the ingestor
        valid_configurations, errors = controller.run(configurations=body)
        if valid_configurations == [] : return jsonify(errors), 422

        # call the modeler
        try:
            output = predict.run(body=valid_configurations)
        except:
            return jsonify("Prediction Error"), 500
        
        return jsonify(output), 200

@app.route("/predict_attitudes", methods=["POST"])
def predict_attitudes():
    
    if request.method == "POST":

        controller = Predict_controller()
        predict = Predict(purpose="Attitudes")
        
        body = request.get_json(force=True, silent=False, cache=True)

        # call the ingestor
        valid_configurations, errors = controller.run(configurations=body)
        if valid_configurations == [] : return jsonify(errors), 422

        # call the modeler
        try:
            output = predict.run(body=valid_configurations)
        except:
            return jsonify("Prediction Error"), 500
        
        return jsonify(output), 200

@app.route("/predict_cultures", methods=["POST"])
def predict_cultures():
    
    if request.method == "POST":

        controller = Predict_controller()
        predict = Predict(purpose="Cultures")
        
        body = request.get_json(force=True, silent=False, cache=True)

        # call the ingestor
        valid_configurations, errors = controller.run(configurations=body)
        if valid_configurations == [] : return jsonify(errors), 422

        # call the modeler
        try:
            output = predict.run(body=valid_configurations)
        except:
            return jsonify("Prediction Error"), 500
        
        return jsonify(output), 200

@app.route("/predict_status", methods=["POST"])
def predict_status():
    
    if request.method == "POST":

        controller = Predict_controller()
        predict = Predict(purpose="Status")
        
        body = request.get_json(force=True, silent=False, cache=True)

        # call the ingestor
        valid_configurations, errors = controller.run(configurations=body)
        if valid_configurations == [] : return jsonify(errors), 422

        # call the modeler
        try:
            output = predict.run(body=valid_configurations)
        except:
            return jsonify("Prediction Error"), 500
        
        return jsonify(output), 200

@app.route("/delete_samples", methods=["DELETE"])
def delete_samples():
    
    if request.method == "DELETE":

        body = request.get_json(force=True, silent=False, cache=True)
        
        controller = Delete_controller()
        response = controller.run(body=body)
        if not response: return "consider sending a correct json", 422

        thread = Thread(target=deleter, args=(body,))
        thread.daemon = True
        thread.start()

        return "deleting done successfully", 200

@app.route("/train_models", methods=["PUT"])
def train_models():
    
    if request.method == "PUT":

        body = request.get_json(force=True, silent=False, cache=True)
        
        controller = Train_controller()
        response = controller.run(body=body)
        if not response: return "consider sending a correct json", 422

        thread = Thread(target=trainer, args=(body,))
        thread.daemon = True
        thread.start()

        return "training done successfully", 200

@app.route("/insert_samples", methods=["POST"])
def insert_samples():
    
    if request.method == "POST":

        body = request.get_json(force=True, silent=False, cache=True)

        controller = Insert_controller()
        response = controller.run(body=body)
        if not response[0]: return jsonify(response[2]), response[1]
        else: samples = response[2]


        thread = Thread(target=inserter, args=(samples,))
        thread.daemon = True
        thread.start()
        
        return "Inserting done successfully", 200

@app.route("/read_samples", methods=["GET"])
def read_samples():
    
    if request.method == "GET":

        read = Read()

        # call the modeler
        try:
            output = read.run()
        except:
            return jsonify("Reading Error"), 500
        
        return jsonify(output), 200

@app.route("/read_metrics", methods=["GET"])
def read_metrics():
    
    if request.method == "GET":

        read = Read_metrics()

        # call the modeler
        try:
            output = read.run()
        except:
            return jsonify("Reading Error"), 500
        
        return jsonify(output), 200


if __name__ == "__main__":
    port = int(os.environ.get('PORT', 3000))
    app.run(debug=True, host='0.0.0.0', port=port)