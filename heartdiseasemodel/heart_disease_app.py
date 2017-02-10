import flask
from sklearn.linear_model import LogisticRegression
import numpy as np
import pandas as pd
from functools import wraps
from flask import request, Response, render_template

#---------- MODEL IN MEMORY ----------------#

# Read the scientific data on heart disease risk,
# Build a LogisticRegression predictor on it
patients = pd.read_csv("UCI_v5.csv",header=None)
patients.columns=['agesex','chol','thalach','num']
X = patients[['agesex','chol','thalach']]
Y = patients['num']
PREDICTOR = LogisticRegression().fit(X,Y)


#---------- URLS AND WEB PAGES -------------#
def check_auth(username, password):
    """This function is called to check if a username /
    password combination is valid.
    """
    return username == 'admin' and password == '123'

def authenticate():
    """Sends a 401 response that enables basic auth"""
    return Response(
    'Could not verify your access level for that URL.\n'
    'You have to login with proper credentials', 401,
    {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated


# Initialize the app
app = flask.Flask(__name__)

# Homepage
@app.route("/")
def viz_page():
    """ Homepage: serve our visualization page, awesome.html
    """
    with open("heartDiseaseModel.html", 'r') as viz_file:
        return viz_file.read()

# Get an example and return it's score from the predictor model
@app.route("/score", methods=["POST"])
def score():
    """  When A POST request with json data is made to this uri,
         Read the example from the json, predict probability and
         send it with a response
    """
    # Get decision score for our example that came with the request
    data = flask.request.json
    x = np.matrix(data["example"])
    score = PREDICTOR.predict_proba(x)
    # Put the result in a nice dict so we can send it as json
    results = {"score": score[0,1]}
    return flask.jsonify(results)

#--------- RUN WEB APP SERVER ------------#

# Start the app server on port 632
# (The default website port)
app.run(host='0.0.0.0', port=632)
#app.run()

