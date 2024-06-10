import json
import os

import numpy as np
import pandas as pd
import scipy.stats as st

from tensorflow.keras.models import Sequential, model_from_json
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    roc_auc_score,
    average_precision_score,
    f1_score,
    recall_score,
    precision_score,
    balanced_accuracy_score,
    roc_curve,
    brier_score_loss,
    roc_auc_score,
)

from data_preprocess_hybrid_sw import load_data

x_train, y_train, x_test, y_test = load_data(7)

model_dir = 'train_bilstm_sw/0.382 2x32 1x1024' 

model = keras.models.model_from_json(open(model_dir + '/model.json', 'rt').read())
model.load_weights(model_dir + '/tunned_weights.hdf5')
y_pred = model.predict(x_test).flatten() 

preds_ids = pd.DataFrame({'predictions': y_pred, 'ground_truth': y_test})
preds_ids.to_csv('preds_gt_bilstm_mv.csv')

fpr, tpr, thvals = roc_curve(y_test, y_pred)
thval = thvals[np.argmax(tpr - fpr)] 
print('optimal thval: {}'.format(thval))

y_pred_bin = y_pred > thval 

METRICS = {'roc_auc' : roc_auc_score,
           'auprc': average_precision_score,
          'f1_score' : f1_score,
          'precision_score' : precision_score,
          'recall_score' : recall_score,
          'balanced_accuracy_score' : balanced_accuracy_score,
          'brier_score_loss' : brier_score_loss}
rng_seed = 42  
rng = np.random.RandomState(rng_seed)

metric_data = []
for metric_name, metric_func in METRICS.items():
    y_pred_boot = y_pred if metric_name in ('roc_auc', 'brier_score_loss', 'auprc') else y_pred_bin
    vals = []
    for iboot in range(1000): 
        boot_idx = np.random.choice(len(y_test), len(y_test)//2, replace=False)
        vals.append(metric_func(y_test[boot_idx], y_pred_boot[boot_idx]))
    m = np.mean(vals)
    s = st.sem(vals)  
    l, h = st.t.interval(confidence=0.95, df=len(vals)-1, loc=m, scale=s) 
    print(f'{metric_name}: {m:.3f} ({l:.3f} - {h:.3f})')
    metric_data.append({'Metric': metric_name, 'Mean': f'{m:.3f}', 'Lower Bound': f'{l:.3f}', 'Upper Bound': f'{h:.3f}'})
    metric_df = pd.DataFrame(metric_data)
metric_df.to_csv('metrics_bilstm_sw.csv', index=False)
