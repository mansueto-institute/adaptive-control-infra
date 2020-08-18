from flask import Flask, request
import arviz as az
import matplotlib.pyplot as plt
import numpy as np

import pymc3 as pm
from pymc3 import  *


app = Flask(__name__)


@app.route('/', methods=['GET'])
def main():

    print('Running on PyMC3 v{}'.format(pm.__version__))

    size = 200
    true_intercept = 1
    true_slope = 2

    x = np.linspace(0, 1, size)
   
    true_regression_line = true_intercept + true_slope * x           # y = a + b*x
    y = true_regression_line + np.random.normal(scale=.5, size=size) # add noise

    data = dict(x=x, y=y)

    with Model() as model: 
    
        # Define priors
        sigma = HalfCauchy('sigma', beta=10, testval=1.)
        intercept = Normal('Intercept', 0, sigma=20)
        x_coeff = Normal('x', 0, sigma=20)

        # Define likelihood
        likelihood = Normal('y', mu=intercept + x_coeff * x,
                            sigma=sigma, observed=y)

        # Inference!
        trace = sample(3000) # draw 3000 posterior samples using NUTS sampling
        
    return "OK"

if __name__ == '__main__':

    # Used when running locally only. When deploying to Cloud Run,
    # a webserver process such as Gunicorn will serve the app.
    app.run(host='localhost', port=8080, debug=True)
