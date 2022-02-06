import pandas as pd
from datetime import datetime
import joblib
import mlflow

from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.decomposition import PCA
from sklearn.linear_model import LogisticRegression

from utils.files_util import load_files

def fit_best_model():

"""     mlflow.set_tracking_uri('http://localhost:5000')
    experiment_name = "Breast cancer model"
    if not mlflow.get_experiment_by_name(experiment_name):
        mlflow.create_experiment(name=experiment_name) 
    experiment = mlflow.get_experiment_by_name(experiment_name)
    mlflow.sklearn.autolog()
    
    with mlflow.start_run(experiment_id = experiment.experiment_id): """
        df, best_params = load_files(['df', 'exp_info'])
        pipe = Pipeline([('scaler', StandardScaler()),
                    ('pca', PCA(n_components = best_params['best_pca_components'].values[0])),
                    ('log_reg', LogisticRegression(C=best_params['best_logreg_c'].values[0]))
                    ])     
        pipe.fit(df.iloc[:,:-1], df['label'])

        # save best model
        now = datetime.now().strftime('%d-%m-%Y_%H:%M:%S')
        filename = 'model_' + now + '.pkl'
        joblib.dump(pipe, '/opt/airflow/models/' + filename, compress=1)
